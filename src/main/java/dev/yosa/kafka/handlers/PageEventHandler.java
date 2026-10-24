package dev.yosa.kafka.handlers;

import dev.yosa.kafka.events.PageEvent;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PageEventHandler {

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> System.out.println("****************** " + input);
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            String page = Math.random() > 0.5 ? "P1" : "P2";
            String user = Math.random() > 0.5 ? "U1" : "U2";
            long duration = 10 + new Random().nextInt(10000);
            return new PageEvent(page, user, new Date(), duration);
        };
    }
}
