package com.tj.user.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Account {
    private UUID id;
    private UUID userId;
    private String currency;
    private BigDecimal balance;
    private BigDecimal reservedBalance;
    private Instant createdAt;
    private Instant updatedAt;
}
