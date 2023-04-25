package com.harness;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Healthcheck {
    
    @GetMapping("/health")
    public String health() {
        return "OK";
    }
}
