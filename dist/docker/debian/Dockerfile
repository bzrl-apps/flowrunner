FROM debian:bullseye-slim AS builder

RUN mkdir -p /app
WORKDIR /app

COPY flowrunner-*.tar.gz /app
RUN ls -la && pwd
RUN tar -xzf flowrunner-*-$(dpkg --print-architecture)-linux.tar.gz

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y ca-certificates tzdata systemd && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/plugins

COPY --from=builder /app/flowrunner /app/flowrunner
COPY --from=builder /app/*.so /app/plugins/

RUN chmod +x /app/flowrunner

WORKDIR /app
VOLUME /app/flows/

#RUN ./flowrunner server
