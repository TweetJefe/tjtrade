package com.tj.user.repository;

import com.tj.user.dto.UserRegisterRequest;
import com.tj.user.model.User;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;
import static com.tj.user.jooq.Tables.USERS;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class UserRepository {
    private final DSLContext dsl;

    public boolean existsById(UUID userId){
        return dsl.fetchExists(
                dsl.selectFrom(USERS)
                        .where(USERS.ID.eq(userId))
        );
    }

    public boolean existsByEmail(String email){
        return dsl.fetchExists(
                dsl.selectFrom(USERS)
                        .where(USERS.EMAIL.eq(email))
        );
    }

    public boolean existsByUsername(String username){
        return dsl.fetchExists(
                dsl.selectFrom(USERS)
                        .where(USERS.USERNAME.eq(username))
        );
    }

    public User createUser(UserRegisterRequest request, String hashedPassword){
        var record = dsl.insertInto(USERS)
                .set(USERS.ID, UUID.randomUUID())
                .set(USERS.EMAIL, request.email())
                .set(USERS.USERNAME, request.username())
                .set(USERS.PASSWORD_HASH, hashedPassword)
                .set(USERS.CREATED_AT, OffsetDateTime.now())
                .set(USERS.UPDATED_AT, OffsetDateTime.now())
                .set(USERS.STATUS, "ACTIVE") //поменять на KYC_Pending потом
                .returning()
                .fetchOne();

        return record.into(User.class);
    }

    public Optional<User> findById(UUID id) {
        var record = dsl.selectFrom(USERS)
                .where(USERS.ID.eq(id))
                .fetchOne();
        return Optional.ofNullable(record).map(r -> r.into(User.class));
    }

    public int deleteUser(UUID id) {
        return dsl.update(USERS)
                .set(USERS.STATUS, "DELETED")
                .set(USERS.UPDATED_AT, OffsetDateTime.now())
                .where(USERS.ID.eq(id))
                .execute();
    }

    public int setStatusActive(UUID id) {
        return dsl.update(USERS)
                .set(USERS.STATUS, "ACTIVE")
                .set(USERS.UPDATED_AT, OffsetDateTime.now())
                .where(USERS.ID.eq(id))
                .execute();
    }

    public Optional<User> findByEmail(String email){
        var record = dsl.selectFrom(USERS)
                .where(USERS.EMAIL.eq(email))
                .fetchOne();
        return Optional.ofNullable(record).map(r -> r.into(User.class));
    }
}
