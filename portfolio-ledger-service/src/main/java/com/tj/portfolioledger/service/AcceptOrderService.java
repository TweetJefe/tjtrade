package com.tj.portfolioledger.service;

import com.tj.common.enums.OrderSide;
import com.tj.common.enums.OrderType;
import com.tj.portfolioledger.command.AcceptOrderCommand;
import com.tj.portfolioledger.command.ReserveFundsCommand;
import com.tj.portfolioledger.enums.AcceptOrderOutcome;
import com.tj.portfolioledger.exception.AcceptOrderException;
import com.tj.portfolioledger.model.Order;
import com.tj.portfolioledger.repository.FundReservationRepository;
import com.tj.portfolioledger.repository.OrderRepository;
import com.tj.portfolioledger.result.AcceptOrderResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

@Service
public class AcceptOrderService {

    private static final BigDecimal ORDER_VALUE_UPPER_BOUND = new BigDecimal("10000000000");
    private static final BigDecimal RESERVATION_UPPER_BOUND = new BigDecimal("100000000000000000000");
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("[A-Z0-9]+_[A-Z0-9]+");

    private final OrderRepository orderRepository;
    private final FundReservationRepository reservationRepository;
    private final ReserveFundsService reserveFundsService;
    private final Set<String> supportedSymbols;

    public AcceptOrderService(
            OrderRepository orderRepository,
            FundReservationRepository reservationRepository,
            ReserveFundsService reserveFundsService,
            @Value("${tjtrade.trading.symbols}") List<String> supportedSymbols
    ) {
        if (supportedSymbols == null || supportedSymbols.isEmpty()) {
            throw new IllegalArgumentException("At least one trading symbol must be configured");
        }

        supportedSymbols.forEach(this::validateSymbol);

        this.orderRepository = orderRepository;
        this.reservationRepository = reservationRepository;
        this.reserveFundsService = reserveFundsService;
        this.supportedSymbols = Set.copyOf(supportedSymbols);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public AcceptOrderResult acceptOrder(AcceptOrderCommand command) {
        validate(command);

        ReserveFundsCommand reservation = reservationCommand(command);
        Order order = Order.builder()
                .id(command.orderId())
                .userId(command.userId())
                .symbol(command.symbol())
                .side(command.side())
                .type(command.type())
                .price(command.price())
                .quantity(command.quantity())
                .build();

        if (!orderRepository.insertIfAbsent(order)) {
            return handleExistingOrder(command, reservation);
        }

        if (!supportedSymbols.contains(command.symbol())) {
            throw new AcceptOrderException(
                    AcceptOrderException.Reason.UNSUPPORTED_SYMBOL,
                    "Trading symbol is not supported"
            );
        }

        reserveFundsService.reserveFunds(reservation);

        return new AcceptOrderResult(command.orderId(), AcceptOrderOutcome.ACCEPTED);
    }

    private AcceptOrderResult handleExistingOrder(
            AcceptOrderCommand command,
            ReserveFundsCommand expectedReservation
    ) {
        Order existing = orderRepository.findByOrderIdForUpdate(command.orderId())
                .orElseThrow(() -> new IllegalStateException("Conflicting order was not found"));

        boolean sameParameters = existing.getUserId().equals(command.userId())
                && existing.getSymbol().equals(command.symbol())
                && existing.getSide() == command.side()
                && existing.getType() == command.type()
                && existing.getPrice() != null
                && existing.getPrice().compareTo(command.price()) == 0
                && existing.getQuantity().compareTo(command.quantity()) == 0;

        if (!sameParameters) {
            throw new AcceptOrderException(
                    AcceptOrderException.Reason.ORDER_CONFLICT,
                    "Order ID is already associated with different parameters"
            );
        }

        var reservation = reservationRepository.findByOrderIdForUpdate(command.orderId())
                .orElseThrow(() -> new AcceptOrderException(
                        AcceptOrderException.Reason.INCONSISTENT_RESERVATION,
                        "Existing order has no reservation history"
                ));

        boolean sameReservation = reservation.userId().equals(expectedReservation.userId())
                && reservation.asset().equals(expectedReservation.asset())
                && reservation.amount().compareTo(expectedReservation.amount()) == 0;

        if (!sameReservation) {
            throw new AcceptOrderException(
                    AcceptOrderException.Reason.INCONSISTENT_RESERVATION,
                    "Existing order has inconsistent reservation parameters"
            );
        }

        return new AcceptOrderResult(command.orderId(), AcceptOrderOutcome.ALREADY_ACCEPTED);
    }

    private ReserveFundsCommand reservationCommand(AcceptOrderCommand command) {
        String[] assets = command.symbol().split("_", -1);
        String asset = command.side() == OrderSide.BUY ? assets[1] : assets[0];
        BigDecimal amount = command.side() == OrderSide.BUY
                ? command.price().multiply(command.quantity())
                : command.quantity();

        validateDecimal(amount, "Reservation amount", RESERVATION_UPPER_BOUND);

        return new ReserveFundsCommand(command.orderId(), command.userId(), asset, amount);
    }

    private void validate(AcceptOrderCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("Command is required");
        }

        if (command.orderId() == null || command.userId() == null) {
            throw new IllegalArgumentException("Order ID and user ID are required");
        }

        if (command.side() == null || command.type() == null) {
            throw new IllegalArgumentException("Order side and type are required");
        }

        if (command.type() != OrderType.LIMIT) {
            throw new AcceptOrderException(
                    AcceptOrderException.Reason.UNSUPPORTED_ORDER_TYPE,
                    "Only LIMIT orders are supported"
            );
        }

        validateSymbol(command.symbol());
        validateDecimal(command.price(), "Price", ORDER_VALUE_UPPER_BOUND);
        validateDecimal(command.quantity(), "Quantity", ORDER_VALUE_UPPER_BOUND);
    }

    private void validateSymbol(String symbol) {
        if (symbol == null || symbol.length() > 20 || !SYMBOL_PATTERN.matcher(symbol).matches()) {
            throw new IllegalArgumentException("Symbol must use the BASE_QUOTE format");
        }

        String[] assets = symbol.split("_", -1);

        if (assets[0].equals(assets[1])) {
            throw new IllegalArgumentException("Base and quote assets must be different");
        }
    }

    private void validateDecimal(BigDecimal value, String name, BigDecimal upperBound) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }

        if (value.stripTrailingZeros().scale() > 8) {
            throw new IllegalArgumentException(name + " must have at most 8 decimal places");
        }

        if (value.compareTo(upperBound) >= 0) {
            throw new IllegalArgumentException(name + " exceeds the supported range");
        }
    }
}
