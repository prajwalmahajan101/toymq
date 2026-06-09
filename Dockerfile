# syntax=docker/dockerfile:1.7

# ---- Builder stage --------------------------------------------------
# Pinned to the toolchain in go.mod (1.26.x). Stdlib-only project, so
# CGO_ENABLED=0 is safe and lets the runtime stage be FROM scratch.
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Cache module downloads across rebuilds when go.sum is stable.
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/toymq ./cmd/toymq

# ---- Runtime stage --------------------------------------------------
# scratch keeps the image ~10 MB and means there is literally nothing
# to attack inside the container — no shell, no libc, no package
# manager. The broker is a single static binary.
FROM scratch
COPY --from=builder /out/toymq /toymq

# Default broker data directory; mount a volume here for persistence
# across container restarts.
VOLUME ["/data"]

# Default broker listen port (matches config.DefaultAddr ":6789").
EXPOSE 6789

ENTRYPOINT ["/toymq", "--addr", ":6789", "--data-dir", "/data"]
