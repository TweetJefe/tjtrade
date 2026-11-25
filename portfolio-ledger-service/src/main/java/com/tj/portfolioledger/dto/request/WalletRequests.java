package com.tj.portfolioledger.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public final class WalletRequests {

    private WalletRequests() {
    }

    public record CreateWalletRequest(
            @NotBlank
            @Schema(description = "Asset code; the wallet is created with zero balances", allowableValues = {"BTC", "ETH", "USDT"}, example = "USDT", requiredMode = Schema.RequiredMode.REQUIRED)
            String currency
    ) {
    }
}
