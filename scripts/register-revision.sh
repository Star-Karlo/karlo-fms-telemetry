#!/usr/bin/env bash
# Register a new revision of an ECS task definition with its (single)
# container's image replaced, printing the new revision number.
#
#   register-revision.sh <family> <image>
#
# Copies the latest active revision so every other setting — roles, secrets,
# env, logging, ports — carries forward unchanged; only the image moves. Used
# by the Jakarta deploy, where the registry's tags are immutable and each
# build must be pinned by tag rather than reached through a moving :latest.
set -euo pipefail
FAMILY="$1"; IMAGE="$2"
aws ecs describe-task-definition --task-definition "$FAMILY" --query 'taskDefinition' > /tmp/td.json
python3 - "$IMAGE" <<'PY'
import json, sys
td = json.load(open('/tmp/td.json'))
assert len(td['containerDefinitions']) == 1, "expected exactly one container"
td['containerDefinitions'][0]['image'] = sys.argv[1]
for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities',
          'registeredAt','registeredBy','deregisteredAt'):
    td.pop(k, None)
json.dump(td, open('/tmp/td-new.json', 'w'))
PY
aws ecs register-task-definition --cli-input-json file:///tmp/td-new.json \
  --query 'taskDefinition.revision' --output text
