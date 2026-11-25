package com.tj.user.grpc;

import com.tj.common.grpc.CheckUserRequest;
import com.tj.common.grpc.CheckUserResponse;
import com.tj.user.model.User;
import com.tj.user.repository.UserRepository;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UserServiceGrpcImplTest {

    @Test
    void returnsPersistedStatusIncludingClosedAndDeleted() {
        for (String status : new String[]{"ACTIVE", "CLOSED", "DELETED"}) {
            var repository = mock(UserRepository.class);
            var userId = UUID.randomUUID();
            when(repository.findById(userId)).thenReturn(Optional.of(User.builder().id(userId).status(status).build()));
            var response = new AtomicReference<CheckUserResponse>();
            new UserServiceGrpcImpl(repository).checkUserExists(CheckUserRequest.newBuilder().setUserId(userId.toString()).build(), observer(response));
            assertTrue(response.get().getExists());
            assertEquals(status, response.get().getStatus());
        }
    }

    @Test
    void missingUserIsReportedAsMissing() {
        var repository = mock(UserRepository.class);
        var userId = UUID.randomUUID();
        when(repository.findById(userId)).thenReturn(Optional.empty());
        var response = new AtomicReference<CheckUserResponse>();
        new UserServiceGrpcImpl(repository).checkUserExists(CheckUserRequest.newBuilder().setUserId(userId.toString()).build(), observer(response));
        assertFalse(response.get().getExists());
        assertEquals("NOT_FOUND", response.get().getStatus());
    }

    @Test
    void malformedIdentityFailsBeforeRepositoryCall() {
        var repository = mock(UserRepository.class);
        var failure = new AtomicReference<Throwable>();
        new UserServiceGrpcImpl(repository).checkUserExists(CheckUserRequest.newBuilder().setUserId("bad").build(), new StreamObserver<>() {
            public void onNext(CheckUserResponse response) { fail("No successful response expected"); }
            public void onCompleted() { fail("No successful completion expected"); }
            public void onError(Throwable error) { failure.set(error); }
        });
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(failure.get()).getCode());
        verifyNoInteractions(repository);
    }

    private StreamObserver<CheckUserResponse> observer(AtomicReference<CheckUserResponse> response) {
        return new StreamObserver<>() {
            public void onNext(CheckUserResponse value) { response.set(value); }
            public void onCompleted() { }
            public void onError(Throwable error) { fail(error); }
        };
    }
}
