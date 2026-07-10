# Multi-stage build for any FMS service binary under ./cmd.
#   docker build --build-arg SERVICE=geocode -t fms-geocode .
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN test -n "$SERVICE" || (echo "SERVICE build-arg required" && exit 1)
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
