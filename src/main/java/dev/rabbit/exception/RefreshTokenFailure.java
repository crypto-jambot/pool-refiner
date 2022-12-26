package dev.rabbit.exception;

public class RefreshTokenFailure extends RuntimeException {
    public RefreshTokenFailure(String message, Throwable throwable) {
        super(message, throwable);
    }
}
