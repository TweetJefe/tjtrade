package com.tj.portfolioledger.config;

import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import tools.jackson.databind.json.JsonMapper;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

@Configuration(proxyBeanMethods = false)
@EnableWebSecurity
@SecurityScheme(name = "walletBearer", type = SecuritySchemeType.HTTP, scheme = "bearer", bearerFormat = "JWT")
public class WalletSecurityConfiguration {

    @Bean
    JwtDecoder walletJwtDecoder(@Value("${jwt.secret}") String encodedSecret) {
        byte[] secret = Base64.getDecoder().decode(encodedSecret);

        if (secret.length < 32) {
            throw new IllegalArgumentException("Wallet JWT secret must contain at least 32 bytes");
        }

        var decoder = NimbusJwtDecoder.withSecretKey(new SecretKeySpec(secret, "HmacSHA256"))
                .macAlgorithm(MacAlgorithm.HS256).build();
        OAuth2TokenValidator<Jwt> claims = jwt -> {
            try {
                if (jwt.getExpiresAt() != null && jwt.getSubject() != null
                        && UUID.fromString(jwt.getSubject()).toString().equals(jwt.getSubject())) {
                    return OAuth2TokenValidatorResult.success();
                }
            } catch (IllegalArgumentException ignored) {

            }

            return OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token", "Invalid wallet identity claims", null));
        };
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(new JwtTimestampValidator(Duration.ofSeconds(30)), claims));

        return decoder;
    }

    @Bean
    @Order(1)
    SecurityFilterChain walletSecurity(HttpSecurity http, JwtDecoder walletJwtDecoder) throws Exception {
        return http.securityMatcher("/api/v1/wallets", "/api/v1/wallets/**")
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.anyRequest().authenticated())
                .oauth2ResourceServer(oauth -> oauth
                        .jwt(jwt -> jwt.decoder(walletJwtDecoder))
                        .authenticationEntryPoint((request, response, exception) -> problem(response, HttpStatus.UNAUTHORIZED))
                        .accessDeniedHandler((request, response, exception) -> problem(response, HttpStatus.FORBIDDEN)))
                .build();
    }

    @Bean
    @Order(2)
    SecurityFilterChain legacyHttpSecurity(HttpSecurity http) throws Exception {
        // Legacy portfolio endpoints retain their existing policy until the security migration.
        return http.csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
    }

    private void problem(HttpServletResponse response, HttpStatus status) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);

        if (status == HttpStatus.UNAUTHORIZED) {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        }

        var body = ProblemDetail.forStatusAndDetail(status, status == HttpStatus.UNAUTHORIZED
                ? "A valid bearer token is required" : "Access to this wallet operation is denied");
        body.setTitle(status.getReasonPhrase());
        response.getWriter().write(JsonMapper.builder().build().writeValueAsString(body));
    }
}
