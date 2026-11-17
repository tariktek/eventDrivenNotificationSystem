# syntax=docker/dockerfile:1

# ---- build -------------------------------------------------------------------
FROM golang:1.27-alpine AS build

WORKDIR /src
ENV CGO_ENABLED=0

# Download modules in their own layer so code changes don't re-fetch them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -e; \
    pkg=github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo; \
    for bin in api worker mockprovider healthcheck; do \
      go build -trimpath \
        -ldflags "-s -w -X ${pkg}.Version=${VERSION} -X ${pkg}.Commit=${COMMIT}" \
        -o /out/${bin} ./cmd/${bin}; \
    done

# ---- runtime -----------------------------------------------------------------
# distroless/static: no shell or package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ /app/

USER nonroot:nonroot
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=4s --start-period=10s --retries=3 \
  CMD ["/app/healthcheck", "http://127.0.0.1:8080/health"]

# One image, several binaries: compose selects the worker via `entrypoint`.
ENTRYPOINT ["/app/api"]
