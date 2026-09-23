package com.tj.user.dto;

public record UserLoginRequest(
        String email,
        String password
) {}
