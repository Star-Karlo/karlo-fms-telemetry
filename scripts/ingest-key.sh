#!/usr/bin/env bash
# Mint an ingest key for a data sender (a tracker vendor, a partner gateway)
# — or list / rename / revoke — by running /ingestkey inside the VPC.
#
#   scripts/ingest-key.sh create <name>    prints nothing; the key goes to SSM
#                                          /karlo-fms/ingest-key-<name> (SecureString)
#   scripts/ingest-key.sh list
#   scripts/ingest-key.sh rename <id> <new-name>
#   scripts/ingest-key.sh revoke <id>
#
# Keys are stored as sha256 only: a lost key is reissued, never recovered.
# Each sender gets its own key so revoking one never touches another.
# The one-off task's log stream (the only place the plaintext ever appears)
# is deleted right after the key is read into SSM.
set -euo pipefail
export AWS_PROFILE=${AWS_PROFILE:-Karlo} AWS_DEFAULT_REGION=ap-southeast-3
CLUSTER=karlo-prod; FAMILY=karlo-prod-fms-dbtool
cmd=${1:?usage: ingest-key.sh create <name> | list | rename <id> <name> | revoke <id>}; shift || true
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)

# A dbtool revision whose entrypoint is /ingestkey (ECS cannot override an
# entrypoint per run), then back to /telmigrate so the family stays what it is.
aws ecs describe-task-definition --task-definition "$FAMILY" --query taskDefinition > "$tmp/td.json"
python3 - "$tmp" <<'PY'
import json,sys
td=json.load(open(sys.argv[1]+'/td.json')); c=td['containerDefinitions'][0]
for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy','deregisteredAt'): td.pop(k,None)

# DATABASE_URL, pointed at the SAME parameter the running services use.
#
# WHY THIS IS HERE. ingestkey falls back to DST_DSN when DATABASE_URL is
# unset, and DST_DSN is telemetry-rds-admin-url: the admin credential from
# the September relocation, whose password no longer authenticates
# (SQLSTATE 28P01, seen 2026-10-27). telemetry_app does authenticate, and
# db/rds/schema.sql grants it SELECT/INSERT/UPDATE/DELETE on every table in
# public, so it can list, create, rename and revoke.
#
# The dbtool family keeps SRC_DSN/DST_DSN/APP_DSN untouched: those belong to
# telmigrate, and this entrypoint is a visitor. The added secret goes into
# the /ingestkey revision only, which is why it is removed again before the
# restore revision is written.
base = next((x['valueFrom'] for x in c.get('secrets') or [] if x['name']=='DST_DSN'), None)
added = False
if base and not any(x['name']=='DATABASE_URL' for x in c.get('secrets') or []):
    app = base.rsplit('/',1)[0] + '/telemetry-database-url'
    c.setdefault('secrets',[]).append({'name':'DATABASE_URL','valueFrom':app})
    added = True

c['entryPoint']=['/ingestkey']; c['command']=['list']; json.dump(td,open(sys.argv[1]+'/key.json','w'))
if added:
    c['secrets']=[x for x in c['secrets'] if x['name']!='DATABASE_URL']
c['entryPoint']=['/telmigrate']; c['command']=['verify']; json.dump(td,open(sys.argv[1]+'/back.json','w'))
PY
rev=$(aws ecs register-task-definition --cli-input-json "file://$tmp/key.json" --query 'taskDefinition.revision' --output text)
trap 'aws ecs register-task-definition --cli-input-json "file://$tmp/back.json" >/dev/null; rm -rf "$tmp"' EXIT

net=$(cd "$here/../infra/jakarta" && terraform output -raw migrate_network)
args=$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1:]))' "$cmd" "$@")
arn=$(aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE --task-definition "$FAMILY:$rev" \
  --network-configuration "$net" --overrides "{\"containerOverrides\":[{\"name\":\"dbtool\",\"command\":$args}]}" \
  --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$arn"
code=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$arn" --query 'tasks[0].containers[0].exitCode' --output text)
stream="ecs/dbtool/${arn##*/}"
out=$(aws logs get-log-events --log-group-name "/ecs/$FAMILY" --log-stream-name "$stream" --start-from-head --query 'events[].message' --output text | tr '\t' '\n')

if [ "$cmd" = create ]; then
  name=$1
  key=$(printf '%s\n' "$out" | grep -E '^\s+[A-Za-z0-9_+/=-]{24,}\s*$' | head -1 | tr -d ' ')
  aws logs delete-log-stream --log-group-name "/ecs/$FAMILY" --log-stream-name "$stream"
  [ -n "$key" ] || { echo "no key in output (exit $code):"; printf '%s\n' "$out" | sed -E 's/[A-Za-z0-9_+\/=-]{24,}/<key>/g'; exit 1; }
  aws ssm put-parameter --name "/karlo-fms/ingest-key-$name" --type SecureString --overwrite --value "$key" \
    --description "X-Ingest-Key for $name, minted $(date -u +%F)" >/dev/null
  printf '%s\n' "$out" | grep -E 'created|id=' | head -1
  echo "Stored in SSM: /karlo-fms/ingest-key-$name"
  echo "  aws ssm get-parameter --region ap-southeast-3 --name /karlo-fms/ingest-key-$name --with-decryption --query Parameter.Value --output text"
else
  printf '%s\n' "$out"
  aws logs delete-log-stream --log-group-name "/ecs/$FAMILY" --log-stream-name "$stream" >/dev/null 2>&1 || true
fi
[ "$code" = 0 ]
