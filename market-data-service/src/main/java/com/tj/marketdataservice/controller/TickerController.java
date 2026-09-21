package com.tj.marketdataservice.controller;



import com.tj.marketdataservice.service.TickerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1/ticker")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class TickerController {
    private final TickerService service;

    @GetMapping("/stream/{symbol}")
    public SseEmitter getStreamForSymbol(@PathVariable String symbol){
        return service.subscribe(symbol);
    }
}
