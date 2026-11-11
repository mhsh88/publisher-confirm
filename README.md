# publisher-confirm

Two local labs for RabbitMQ publisher confirms. Java 17. No database.

Run one lab at a time. Both compose files publish host port 5672.

RabbitMQ user and password are `guest` / `guest`, the broker's local default.

## Pekko cluster

Three nodes. Sharded workers publish to each other through RabbitMQ. Dashboard shows which worker on which node handled which message.

```bash
mvn -q package
docker compose up
```

Dashboard: http://localhost:8081/

`docker compose` mounts `node/target/node-1.0-SNAPSHOT.jar`, so package before starting the stack.

## Confirm bench

Standalone Spring Boot 2.5 app. Not a module of the cluster build. One publisher connection, one channel. A message stays unsent until its confirm succeeds, and the publisher walks that set again.

```bash
cd confirm-bench
docker compose up -d
mvn -q spring-boot:run
```

Knobs, with defaults:

```bash
mvn -q spring-boot:run -Dspring-boot.run.arguments='--timeout-ms=1000 --process-ms=5 --send-rate=1000 --duration-seconds=60'
```

| knob | default | meaning |
|---|---|---|
| `timeout-ms` | 1000 | per-message confirm timeout, started after `convertAndSend` returns |
| `process-ms` | 5 | sleep on the confirm reader after a successful ack |
| `send-rate` | 1000 | new messages per second |
| `duration-seconds` | 60 | how long new messages arrive |

Each second prints `second arrived sent success timeout unsent`.

With confirm work `p` seconds (`process-ms / 1000`):

```
s = k + w
k <= 1/p
```

`s` is publish attempts per second, `k` is confirms per second, `w` is timeouts per second. One channel keeps confirm latency near `p`, so a timeout around `5p` stays quiet. Around `3p` and below, timed-out messages are published again and `s` stays above `k`. The unsent set still grows by about `arrival - k` per second.

Pekko is Apache-2.0. This repo is MIT, see `LICENSE`.
