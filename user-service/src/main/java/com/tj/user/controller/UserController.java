package com.tj.user.controller;

import com.google.api.Http;
import com.tj.common.dto.AccountDTO;
import com.tj.common.dto.UserDTO;
import com.tj.user.dto.CreateAccountRequest;
import com.tj.user.dto.UserRegisterRequest;
import com.tj.user.service.AccountService;
import com.tj.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.apache.catalina.connector.Response;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;
    private final AccountService accountService;

    @PostMapping("")
    public ResponseEntity<UserDTO> createUser(
            @RequestBody UserRegisterRequest request){
        return ResponseEntity.status(HttpStatus.CREATED).body(userService.createUser(request));
    }

    @GetMapping("/{id}")
    public ResponseEntity<UserDTO> getUserById(
            @PathVariable UUID id){
        return ResponseEntity.status(HttpStatus.OK).body(userService.getUserById(id));
    }

    @PatchMapping("/{id}/verify")
    public ResponseEntity<Void> verifyUser(
            @PathVariable UUID id){
        userService.verifyUser(id);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteUser(
            @PathVariable UUID id){
        userService.deleteUser(id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
