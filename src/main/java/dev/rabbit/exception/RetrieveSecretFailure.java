package dev.rabbit.exception;

public class RetrieveSecretFailure extends RuntimeException {
    public RetrieveSecretFailure(String message, Throwable throwable) {
        super(message, throwable);
    }

    public RetrieveSecretFailure(String message) {
        super(message);
    }
}
