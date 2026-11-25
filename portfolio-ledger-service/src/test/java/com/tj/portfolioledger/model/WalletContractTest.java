package com.tj.portfolioledger.model;

import com.tj.portfolioledger.dto.response.WalletResponses;
import com.tj.portfolioledger.enums.WalletAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WalletContractTest {

    @ParameterizedTest
    @EnumSource(WalletAsset.class)
    void acceptsSmallestUnitAndLargestStoredBalance(WalletAsset asset) {
        assertEquals(new BigDecimal("0.00000001"), WalletAmounts.requirePositive(asset, new BigDecimal("0.00000001")));
        var maximum = new BigDecimal("99999999999999999999.99999999");
        assertEquals(maximum, WalletAmounts.requireBalance(asset, maximum));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"btc", "usdt", " USDT", "USDT ", "USD", "DOGE"})
    void rejectsUnknownAndNoncanonicalAssetCodes(String currency) {
        assertThrows(IllegalArgumentException.class, () -> WalletAsset.fromCode(currency));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "0.000000001", "1.000000001", "100000000000000000000", "1E+100"})
    void rejectsAmountsThatWouldRequireRoundingOrOverflow(String amount) {
        assertThrows(IllegalArgumentException.class,
                () -> WalletAmounts.requireBalance(WalletAsset.USDT, new BigDecimal(amount)));
    }

    @Test
    void acceptsInsignificantZerosWithoutChangingValue() {
        assertEquals(new BigDecimal("1.00000000"),
                WalletAmounts.requirePositive(WalletAsset.BTC, new BigDecimal("1.000000000")));
        assertEquals(new BigDecimal("100.00000000"),
                WalletAmounts.requirePositive(WalletAsset.USDT, new BigDecimal("1E+2")));
    }

    @Test
    void zeroIsAValidBalanceButNotAnOperationAmount() {
        assertEquals(new BigDecimal("0.00000000"), WalletAmounts.requireBalance(WalletAsset.ETH, BigDecimal.ZERO));
        assertThrows(IllegalArgumentException.class, () -> WalletAmounts.requirePositive(WalletAsset.ETH, BigDecimal.ZERO));
    }

    @Test
    void rejectsMissingAssetAmountAndIdentities() {
        assertThrows(IllegalArgumentException.class, () -> WalletAmounts.requireBalance(null, BigDecimal.ONE));
        assertThrows(IllegalArgumentException.class, () -> WalletAmounts.requireBalance(WalletAsset.BTC, null));
        assertThrows(IllegalArgumentException.class, () -> new WalletResponses.WalletResponse(
                null, UUID.randomUUID(), "BTC", BigDecimal.ZERO, BigDecimal.ZERO));
        assertThrows(IllegalArgumentException.class, () -> new WalletResponses.WalletResponse(
                UUID.randomUUID(), null, "BTC", BigDecimal.ZERO, BigDecimal.ZERO));
    }

    @Test
    void serializesExactDecimalStringsAndComputedTotal() {
        var wallet = new WalletResponses.WalletResponse(UUID.randomUUID(), UUID.randomUUID(), "USDT",
                new BigDecimal("9007199254740993.12345678"), new BigDecimal("0.00000001"));
        var tree = JsonMapper.builder().build().valueToTree(wallet);

        assertEquals("9007199254740993.12345678", tree.get("available").asString());
        assertTrue(tree.get("available").isString());
        assertEquals("0.00000001", tree.get("locked").asString());
        assertTrue(tree.get("locked").isString());
        assertEquals("9007199254740993.12345679", tree.get("total").asString());
        assertTrue(tree.get("total").isString());
        assertEquals(6, tree.size());
        assertFalse(tree.has("balance"));
    }

    @Test
    void computedTotalCanExceedOneStoredColumnRange() {
        var maximum = new BigDecimal("99999999999999999999.99999999");
        var wallet = new WalletResponses.WalletResponse(UUID.randomUUID(), UUID.randomUUID(), "BTC", maximum, maximum);

        assertEquals(new BigDecimal("199999999999999999999.99999998"), wallet.total());
    }
}
