package com.tj.user.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


public record UserRegisterRequest(
        String email,
        String password,
        String username) {}
