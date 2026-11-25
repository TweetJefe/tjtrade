package com.tj.portfolioledger.controller;

import com.tj.portfolioledger.dto.request.WalletRequests;
import com.tj.portfolioledger.dto.response.WalletResponses;
import com.tj.portfolioledger.service.WalletService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
@RequiredArgsConstructor
@Tag(name = "Wallets", description = "Create zero wallets and read authenticated user's balances")
@SecurityRequirement(name = "walletBearer")
@ApiResponses({
        @ApiResponse(responseCode = "401", description = "Missing or invalid bearer token", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
        @ApiResponse(responseCode = "500", description = "Unexpected wallet service failure", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
        @ApiResponse(responseCode = "503", description = "Required service is unavailable", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
})
public class WalletController {

    private final WalletService service;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "createWallet", summary = "Create a zero wallet", description = "Owner comes from JWT subject. An existing wallet is returned without resetting its balances.")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, description = "Only the supported asset code; identity and balances cannot be supplied", content = @Content(schema = @Schema(implementation = WalletRequests.CreateWalletRequest.class)))
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Zero wallet created", content = @Content(schema = @Schema(implementation = WalletResponses.WalletResponse.class))),
            @ApiResponse(responseCode = "200", description = "Existing wallet returned", content = @Content(schema = @Schema(implementation = WalletResponses.WalletResponse.class))),
            @ApiResponse(responseCode = "400", description = "Invalid or unsupported currency or request body", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "403", description = "User is not active", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "404", description = "Authenticated user no longer exists", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "409", description = "Wallet creation conflicts with current database state", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    })
    public ResponseEntity<WalletResponses.WalletResponse> create(@Parameter(hidden = true) @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody WalletRequests.CreateWalletRequest request) {
        var result = service.create(UUID.fromString(jwt.getSubject()), request.currency());

        if (result.created()) {
            return ResponseEntity.created(URI.create("/api/v1/wallets/" + result.wallet().walletId())).body(result.wallet());
        }

        return ResponseEntity.ok(result.wallet());
    }

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "listMyWallets", summary = "List current user's wallets", description = "Returns only wallets owned by JWT subject, ordered by currency and wallet ID. An empty list is valid.")
    @ApiResponse(responseCode = "200", description = "Owned wallets", content = @Content(array = @ArraySchema(schema = @Schema(implementation = WalletResponses.WalletResponse.class))))
    public ResponseEntity<List<WalletResponses.WalletResponse>> list(@Parameter(hidden = true) @AuthenticationPrincipal Jwt jwt) {
        return ResponseEntity.ok(service.list(UUID.fromString(jwt.getSubject())));
    }

    @GetMapping(value = "/{walletId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "getMyWallet", summary = "Get an owned wallet", description = "A nonexistent wallet and another user's wallet both return 404.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Owned wallet", content = @Content(schema = @Schema(implementation = WalletResponses.WalletResponse.class))),
            @ApiResponse(responseCode = "400", description = "Malformed wallet ID", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class))),
            @ApiResponse(responseCode = "404", description = "Owned wallet not found", content = @Content(mediaType = "application/problem+json", schema = @Schema(implementation = ProblemDetail.class)))
    })
    public ResponseEntity<WalletResponses.WalletResponse> get(@Parameter(hidden = true) @AuthenticationPrincipal Jwt jwt,
            @Parameter(description = "Wallet identity", required = true, schema = @Schema(type = "string", format = "uuid")) @PathVariable UUID walletId) {
        return ResponseEntity.ok(service.get(UUID.fromString(jwt.getSubject()), walletId));
    }
}
