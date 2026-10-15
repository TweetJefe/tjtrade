package com.tj.portfolioledger.repository;

import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.model.FundReservation;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

import static com.tj.portfolioledger.jooq.Tables.FUND_RESERVATION;

@Repository
@RequiredArgsConstructor
public class FundReservationRepository {

    private final DSLContext dsl;

    public boolean insertIfAbsent(ReserveFundsCommand command){
        int insertedRows = dsl.insertInto(FUND_RESERVATION)
                .set(FUND_RESERVATION.ORDER_ID, command.orderId())
                .set(FUND_RESERVATION.USER_ID, command.userId())
                .set(FUND_RESERVATION.ASSET, command.asset())
                .set(FUND_RESERVATION.AMOUNT, command.amount())
                .set(FUND_RESERVATION.REMAINING_AMOUNT, command.amount())
                .set(FUND_RESERVATION.STATUS, "ACTIVE")
                .onConflict(FUND_RESERVATION.ORDER_ID)
                .doNothing()
                .execute();

        return insertedRows == 1;
    }

    public Optional<com.tj.portfolioledger.model.FundReservation> findByOrderIdForUpdate(UUID orderId){
        return dsl.selectFrom(FUND_RESERVATION)
                .where(FUND_RESERVATION.ORDER_ID.eq(orderId))
                .forUpdate()
                .fetchOptional()
                .map(record -> new FundReservation(
                        record.get(FUND_RESERVATION.ORDER_ID),
                        record.get(FUND_RESERVATION.USER_ID),
                        record.get(FUND_RESERVATION.ASSET),
                        record.get(FUND_RESERVATION.AMOUNT),
                        record.get(FUND_RESERVATION.STATUS)
                ));
    }
}
