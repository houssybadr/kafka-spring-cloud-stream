package dev.yosa.kafka.controllers;

import dev.yosa.kafka.events.PageEvent;
import java.util.Date;
import java.util.Random;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PageEventController {

    private final StreamBridge streamBridge;

    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @GetMapping("/publish")
    public PageEvent publish(@RequestParam String name, @RequestParam(defaultValue = "R2") String topic) {
        String user = Math.random() > 0.5 ? "U1" : "U2";
        long duration = 10 + new Random().nextInt(10000);
        PageEvent event = new PageEvent(name, user, new Date(), duration);
        streamBridge.send(topic, event);
        return event;
    }
}
