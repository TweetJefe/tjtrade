package com.tj.user.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;


public record AuthResponse(
        String token,
        UUID userId
) {
}
