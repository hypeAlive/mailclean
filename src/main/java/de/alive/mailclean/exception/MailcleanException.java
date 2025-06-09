package de.alive.mailclean.exception;

public abstract class MailcleanException extends Exception {

    public MailcleanException(String message) {
        super(message);
    }
}