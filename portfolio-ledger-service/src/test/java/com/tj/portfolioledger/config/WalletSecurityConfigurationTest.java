package com.tj.portfolioledger.config;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwtException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WalletSecurityConfigurationTest {

    private static final String SECRET = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void acceptsSignedExpiringCanonicalUserIdentity() throws Exception {
        String userId = UUID.randomUUID().toString();
        assertEquals(userId, decoder().decode(token(userId, Instant.now().plusSeconds(300), SECRET, JWSAlgorithm.HS256)).getSubject());
    }

    @Test
    void rejectsExpiredMissingExpiryMissingSubjectAndMalformedSubject() throws Exception {
        for (String subject : new String[]{null, "not-a-uuid", "1-1-1-1-1"}) {
            String value = token(subject, Instant.now().plusSeconds(300), SECRET, JWSAlgorithm.HS256);
            assertThrows(JwtException.class, () -> decoder().decode(value));
        }

        String missingExpiry = token(UUID.randomUUID().toString(), null, SECRET, JWSAlgorithm.HS256);
        String expired = token(UUID.randomUUID().toString(), Instant.now().minusSeconds(300), SECRET, JWSAlgorithm.HS256);
        assertThrows(JwtException.class, () -> decoder().decode(missingExpiry));
        assertThrows(JwtException.class, () -> decoder().decode(expired));
    }

    @Test
    void rejectsWrongSignatureAndNonallowedAlgorithm() throws Exception {
        String subject = UUID.randomUUID().toString();
        String wrongKey = token(subject, Instant.now().plusSeconds(300), "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd", JWSAlgorithm.HS256);
        String wrongAlgorithm = token(subject, Instant.now().plusSeconds(300), SECRET, JWSAlgorithm.HS512);

        assertThrows(JwtException.class, () -> decoder().decode(wrongKey));
        assertThrows(JwtException.class, () -> decoder().decode(wrongAlgorithm));
    }

    @Test
    void rejectsWeakOrMalformedConfiguration() {
        var config = new WalletSecurityConfiguration();
        assertThrows(IllegalArgumentException.class, () -> config.walletJwtDecoder("invalid-base64!"));
        assertThrows(IllegalArgumentException.class, () -> config.walletJwtDecoder(Base64.getEncoder().encodeToString("short".getBytes(StandardCharsets.UTF_8))));
    }

    private org.springframework.security.oauth2.jwt.JwtDecoder decoder() {
        return new WalletSecurityConfiguration().walletJwtDecoder(Base64.getEncoder().encodeToString(SECRET.getBytes(StandardCharsets.UTF_8)));
    }

    private String token(String subject, Instant expiry, String secret, JWSAlgorithm algorithm) throws Exception {
        var claims = new JWTClaimsSet.Builder().subject(subject);

        if (expiry != null) {
            claims.expirationTime(Date.from(expiry));
        }

        var jwt = new SignedJWT(new JWSHeader(algorithm), claims.build());
        jwt.sign(new MACSigner(secret.getBytes(StandardCharsets.UTF_8)));

        return jwt.serialize();
    }
}
