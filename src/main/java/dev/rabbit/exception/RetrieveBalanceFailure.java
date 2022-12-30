package dev.rabbit.exception;

public class RetrieveBalanceFailure extends RuntimeException {
    public RetrieveBalanceFailure(String message, Throwable throwable) {
        super(message, throwable);
    }
}
