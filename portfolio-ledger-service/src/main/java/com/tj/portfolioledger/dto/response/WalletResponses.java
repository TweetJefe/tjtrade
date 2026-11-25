package com.tj.portfolioledger.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tj.portfolioledger.enums.WalletAsset;
import com.tj.portfolioledger.model.WalletAmounts;
import com.tj.portfolioledger.serialization.WalletAmountSerializer;
import tools.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.util.UUID;

public final class WalletResponses {

    private WalletResponses() {
    }

    @Schema(description = "Authoritative wallet balances held by the ledger")
    public record WalletResponse(
            @Schema(description = "Wallet identity", format = "uuid", requiredMode = Schema.RequiredMode.REQUIRED)
            UUID walletId,
            @Schema(description = "Wallet owner identity", format = "uuid", requiredMode = Schema.RequiredMode.REQUIRED)
            UUID userId,
            @Schema(description = "Supported wallet asset", allowableValues = {"BTC", "ETH", "USDT"}, requiredMode = Schema.RequiredMode.REQUIRED)
            String currency,
            @JsonSerialize(using = WalletAmountSerializer.class)
            @Schema(description = "Funds available for reservation or withdrawal; decimal string with 8 fractional digits", type = "string", example = "800.00000000", requiredMode = Schema.RequiredMode.REQUIRED)
            BigDecimal available,
            @JsonSerialize(using = WalletAmountSerializer.class)
            @Schema(description = "Funds held by reservations; decimal string with 8 fractional digits", type = "string", example = "200.00000000", requiredMode = Schema.RequiredMode.REQUIRED)
            BigDecimal locked
    ) {
        public WalletResponse {
            if (walletId == null || userId == null) {
                throw new IllegalArgumentException("Wallet ID and user ID are required");
            }

            WalletAsset asset = WalletAsset.fromCode(currency);
            available = WalletAmounts.requireBalance(asset, available);
            locked = WalletAmounts.requireBalance(asset, locked);
        }

        @JsonProperty(value = "total", access = JsonProperty.Access.READ_ONLY)
        @JsonSerialize(using = WalletAmountSerializer.class)
        @Schema(description = "Computed available plus locked; not an independently stored balance", type = "string", example = "1000.00000000", accessMode = Schema.AccessMode.READ_ONLY, requiredMode = Schema.RequiredMode.REQUIRED)
        public BigDecimal total() {
            return available.add(locked);
        }
    }
}
