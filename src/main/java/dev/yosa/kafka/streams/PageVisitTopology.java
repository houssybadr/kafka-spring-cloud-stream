package dev.yosa.kafka.streams;

import dev.yosa.kafka.events.PageEvent;
import java.time.Duration;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PageVisitTopology {

    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kstreamFunction() {
        return input -> input
                .filter((key, value) -> value.duration() > 100)
                .map((key, value) -> new KeyValue<>(value.name(), value.duration()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.as("count-store"))
                .toStream()
                .map((windowed, count) -> new KeyValue<>(windowed.key(), count));
    }
}
