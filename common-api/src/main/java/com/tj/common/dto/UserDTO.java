package com.tj.common.dto;

import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record UserDTO(
        UUID id,
        String email,
        String username,
        String status,
        Instant createdAt
) {}
