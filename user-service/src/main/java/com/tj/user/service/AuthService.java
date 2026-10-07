package com.tj.user.service;

import com.tj.user.dto.AuthResponse;
import com.tj.user.dto.UserLoginRequest;
import com.tj.user.dto.UserRegisterRequest;
import com.tj.user.exception.UserNotFoundException;
import com.tj.user.model.User;
import com.tj.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {
    private final UserRepository userRepository;
    private final JwtService jwtService;
    private final PasswordEncoder passwordEncoder;

    public AuthResponse registration(UserRegisterRequest request){
        if (userRepository.existsByEmail(request.email())){
            throw new RuntimeException("Email is taken");
        }

        String hashedPassword = passwordEncoder.encode(request.password());

        User user = userRepository.createUser(request, hashedPassword);

        String token = jwtService.GenerateToken(user.getId(), user.getEmail());

        log.info("New user registered: {}", user.getEmail());

        return new AuthResponse(token, user.getId());
    }

    public AuthResponse login (UserLoginRequest request){
        User user = userRepository.findByEmail(request.email())
                .orElseThrow(() -> new UserNotFoundException("User not found!"));

        if (!passwordEncoder.matches(request.password(), user.getPasswordHash())){
            throw new RuntimeException("Incorrect password");
        }

        String token = jwtService.GenerateToken(user.getId(), user.getEmail());

        log.info("User logged in: {}", user.getEmail());
        return new AuthResponse(token, user.getId());
    }
}
