# confirm-bench

Not part of the Pekko cluster. Stop the root compose stack first. This file also binds host port 5672.

```bash
docker compose up -d
mvn -q spring-boot:run
```

Defaults: `timeout-ms=1000`, `process-ms=5`, `send-rate=1000`, `duration-seconds=60`.

```bash
mvn -q spring-boot:run -Dspring-boot.run.arguments='--timeout-ms=1000 --process-ms=5 --send-rate=1000 --duration-seconds=60'
```

Needs Java 17 and RabbitMQ on `localhost:5672` (`guest` / `guest`). See the root README for what the columns mean.
