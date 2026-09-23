package com.tj.user.controller;

import com.tj.user.dto.AuthResponse;
import com.tj.user.dto.UserLoginRequest;
import com.tj.user.dto.UserRegisterRequest;
import com.tj.user.service.AuthService;
import com.tj.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.apache.catalina.connector.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    public ResponseEntity<AuthResponse> register (@RequestBody UserRegisterRequest request){
        try{
            return ResponseEntity.ok(authService.registration(request));
        }catch (Exception e){
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login (@RequestBody UserLoginRequest request){
        try{
            return ResponseEntity.ok(authService.login(request));
        }catch (Exception e){
            return ResponseEntity.status(401).build();
        }
    }

}
