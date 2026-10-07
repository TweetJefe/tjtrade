package com.tj.integration;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MigrationHistoryTest {
    @Test
    void existingSqlChangesetsRemainImmutable() throws Exception {
        Path root = Path.of("..").toAbsolutePath().normalize();
        for (String line : Files.readAllLines(root.resolve("docs/migration-checksums.sha256"))) {
            String[] entry = line.split("  ", 2);
            // Normalize checkout line endings; SQL contents remain frozen.
            String sql = Files.readString(root.resolve(entry[1])).replace("\r\n", "\n");
            String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(sql.getBytes(StandardCharsets.UTF_8)));
            assertEquals(entry[0], actual, "Existing migration changed: " + entry[1] + ". Add the next changeset.");
        }
    }
}
