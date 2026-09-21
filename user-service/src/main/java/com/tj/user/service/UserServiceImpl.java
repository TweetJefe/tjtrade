package com.tj.user.service;


import com.tj.common.dto.UserDTO;
import com.tj.user.dto.UserRegisterRequest;
import com.tj.user.exception.UserNotFoundException;
import com.tj.user.model.User;
import com.tj.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {
    private final UserRepository userRepository;


    @Override
    public UserDTO createUser(UserRegisterRequest request) {
        if (userRepository.existsByEmail(request.getEmail())){
            throw new IllegalArgumentException("There is an account attached to this email");
        }

        if (userRepository.existsByUsername(request.getUsername())){
            throw new IllegalArgumentException("That username is taken!");
        }

        var savedUser = userRepository.createUser(request);

        return new UserDTO(
                savedUser.getId(),
                savedUser.getEmail(),
                savedUser.getUsername(),
                savedUser.getStatus(),
                savedUser.getCreatedAt()
        );
    }

    @Override
    public UserDTO getUserById(UUID id) {
        var user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("User not found")
        );

        return UserDTO.builder()
                .id(user.getId())
                .username(user.getUsername())
                .email(user.getEmail())
                .status(user.getStatus())
                .createdAt(user.getCreatedAt())
                .build();
    }

    @Override
    public void deleteUser(UUID id) {
        if (userRepository.deleteUser(id) == 0){
            throw new UserNotFoundException("User not found");
        }
    }

    @Override
    public void verifyUser(UUID id) {
        User user = userRepository.findById(id).orElseThrow(() -> new UserNotFoundException("User not found"));

        if (user.getStatus().equals("ACTIVE")){
            throw new IllegalStateException("User is already verified");
        }

        if(userRepository.setStatusActive(id) == 0){
            throw new UserNotFoundException("User not found");
        }
    }
}
