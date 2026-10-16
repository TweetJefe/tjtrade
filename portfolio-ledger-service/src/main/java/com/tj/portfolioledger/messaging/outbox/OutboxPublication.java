package com.tj.portfolioledger.messaging.outbox;

public final class OutboxPublication {

    private OutboxPublication() {
    }

    public record Cursor(
            String stream,
            String aggregateKey,
            long lastPublishedSequence
    ) {
    }

    @FunctionalInterface
    public interface Work {

        void execute(Cursor cursor) throws Exception;
    }
}
