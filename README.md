# Kafka + Spring Cloud Stream — real-time page analytics

Event-driven demo built with **Spring Cloud Stream**, **Apache Kafka** and **Kafka Streams**.
It simulates page visits and streams a real-time windowed count per page to the browser.

## Architecture

```
                       +----------------------+
  REST  GET /publish -->|  PageEventController |-- StreamBridge --> topic R2 --> pageEventConsumer (log)
                       +----------------------+

  pageEventSupplier --> topic R3 --> kstreamFunction (Kafka Streams) --> topic R4
        (poll every 200ms)             | filter / map / groupByKey
                                       | windowedBy(5s) / count
                                       v
                              state store "count-store"
                                       |
                GET /analytics (SSE) <--+   InteractiveQueryService
                                       |
                                browser index.html (smoothie.js chart)
```

## Topics

| Topic | Role                                        |
|-------|---------------------------------------------|
| R2    | events published from the REST controller   |
| R3    | continuous stream produced by the supplier  |
| R4    | windowed counts produced by Kafka Streams   |

## Prerequisites

- JDK 21
- Docker (or any Kafka broker on `localhost:9092`)

## Run

```bash
# 1. Start Kafka (KRaft, single node)
docker compose up -d

# 2. Start the application
mvn spring-boot:run
```

## Try it

- `GET http://localhost:8080/publish?name=P1&topic=R2` — publish one `PageEvent`
- `GET http://localhost:8080/analytics` — Server-Sent Events stream of 5s-window counts
- `GET http://localhost:8080/` — live chart

The consumer `pageEventConsumer` logs every event it receives from `R2` to the console.

## How it works

- `PageEvent` is a record with a page name, a user, a visit date and a visit duration.
- `PageEventHandler` exposes a `Consumer<PageEvent>` and a `Supplier<PageEvent>`.
- `PageVisitTopology` defines a `Function<KStream<String, PageEvent>, KStream<String, Long>>` that
  filters, re-keys by page name and counts visits in 5-second tumbling windows.
- `AnalyticsController` reads the `count-store` materialized view and pushes it to the browser over SSE.

The binder abstraction of Spring Cloud Stream keeps the business code independent from the broker:
swapping Kafka for RabbitMQ only changes the configuration and dependencies.
