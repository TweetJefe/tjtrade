package com.tj.portfolioledger.result;

import com.tj.portfolioledger.dto.response.WalletResponses;

public final class WalletResults {

    private WalletResults() {
    }

    public record Creation(
            WalletResponses.WalletResponse wallet,
            boolean created
    ) {
    }
}
