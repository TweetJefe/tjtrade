package com.tj.matchingengine.controller;

import com.tj.matchingengine.service.MatchingEngineService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/matching")
@RequiredArgsConstructor
public class MatchingEngineController {

    private final MatchingEngineService matchingEngineService;

}
