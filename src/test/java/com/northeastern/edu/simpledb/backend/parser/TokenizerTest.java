package com.northeastern.edu.simpledb.backend.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TokenizerTest {

    @Mock
    Exception err;

    @InjectMocks
    Tokenizer tokenizer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testValidInput_expectedNoException() {
        String s = "SELECT * FROM employee WHERE name = 'jack'";
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Tokenizer tokenizer = new Tokenizer(bytes);

        assertDoesNotThrow(() -> {
            assertEquals("SELECT", tokenizer.peek());
            tokenizer.pop();

            assertEquals("*", tokenizer.peek());
            tokenizer.pop();
        });
    }

    @Test
    void testInValidInput_expectedRuntimeException() {
        String s = "  ^   ";
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Tokenizer tokenizer = new Tokenizer(bytes);

        assertThrows(RuntimeException.class, () -> {
            String token;
            while (!"".equals(token = tokenizer.peek())) {
                System.out.println("token = " + token);
                tokenizer.pop();
            }
        });
    }

}
