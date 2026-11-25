package com.tj.portfolioledger.serialization;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;

import java.math.BigDecimal;

public class WalletAmountSerializer extends ValueSerializer<BigDecimal> {

    @Override
    public void serialize(BigDecimal amount, JsonGenerator generator, SerializationContext context) {
        generator.writeString(amount.toPlainString());
    }
}
