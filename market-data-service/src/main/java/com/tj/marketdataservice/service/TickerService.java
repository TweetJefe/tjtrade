package com.tj.marketdataservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class TickerService {
    private final Map<String, List<SseEmitter>> emitters = new HashMap<>();

    public SseEmitter subscribe(String symbol){
        SseEmitter emitter = new SseEmitter(0L);

        emitters.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>()).add(emitter);
        log.info("New Sse client subscribed to ticker: {}", symbol);

        Runnable removeCallback = () -> removeEmitter(symbol, emitter);

        emitter.onCompletion(removeCallback);
        emitter.onTimeout(removeCallback);
        emitter.onError(e -> removeCallback.run());

        return emitter;
    }

    public void pushPrice(String symbol, BigDecimal price){
        List<SseEmitter> symbolEmitter = emitters.getOrDefault(symbol, List.of());

        for (SseEmitter emitter : symbolEmitter){
            try{
                emitter.send(SseEmitter.event()
                        .name("price-update")
                        .data(price));
            }catch (Exception e){
                emitter.completeWithError(e);
            }
        }
    }

    private void removeEmitter (String symbol, SseEmitter emitter){
        List <SseEmitter> list = emitters.get(symbol);
        if(list != null){
            list.remove(emitter);
        }
    }
}
