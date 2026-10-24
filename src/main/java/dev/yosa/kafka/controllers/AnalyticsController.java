package dev.yosa.kafka.controllers;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class AnalyticsController {

    private final InteractiveQueryService interactiveQueryService;

    public AnalyticsController(InteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping(value = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics() {
        return Flux.interval(Duration.ofSeconds(1)).map(tick -> lastFiveSecondsCounts());
    }

    private Map<String, Long> lastFiveSecondsCounts() {
        ReadOnlyWindowStore<String, Long> store =
                interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());
        Instant now = Instant.now();
        Instant from = now.minusSeconds(5);
        Map<String, Long> counts = new HashMap<>();
        try (KeyValueIterator<Windowed<String>, Long> iterator = store.fetchAll(from, now)) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<String>, Long> entry = iterator.next();
                counts.put(entry.key.key(), entry.value);
            }
        }
        return counts;
    }
}
