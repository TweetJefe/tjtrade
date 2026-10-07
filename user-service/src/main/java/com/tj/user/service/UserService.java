package com.tj.user.service;

import com.tj.common.dto.UserDTO;
import com.tj.user.dto.UserRegisterRequest;
import org.jspecify.annotations.Nullable;

import java.util.UUID;

public interface UserService {

    UserDTO getUserById(UUID id);

    void deleteUser(UUID id);

    void verifyUser(UUID id);
}
