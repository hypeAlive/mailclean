package de.alive.mailclean.exception;

public class MailConnectionException extends Exception {

    private final ConnectionStage stage;

    public enum ConnectionStage {
        POOL_EXHAUSTED,
        POOL_ACQUISITION,
        CONNECTION_CREATION,
        CONNECTION_RETRY,
        CONNECTION_ESTABLISHMENT,
        AUTHENTICATION,
        NETWORK_ERROR,
        CONFIGURATION_ERROR,
        TIMEOUT
    }

    public MailConnectionException(String message, ConnectionStage stage, Throwable cause) {
        super(message, cause);
        this.stage = stage;
    }

    public MailConnectionException(String message, ConnectionStage stage) {
        super(message);
        this.stage = stage;
    }

    public ConnectionStage getStage() {
        return stage;
    }

    public boolean isRecoverable() {
        return stage != ConnectionStage.CONFIGURATION_ERROR &&
                stage != ConnectionStage.AUTHENTICATION;
    }

    @Override
    public String toString() {
        return String.format("MailConnectionException{stage=%s, message='%s'}",
                stage, getMessage());
    }
}