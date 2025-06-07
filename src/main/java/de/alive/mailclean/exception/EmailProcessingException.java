package de.alive.mailclean.exception;

public class EmailProcessingException extends Exception {

    private final String emailId;
    private final String folder;
    private final ProcessingStage stage;

    public enum ProcessingStage {
        CONTENT_EXTRACTION,
        FOLDER_LISTING,
        MESSAGE_PROCESSING,
        MESSAGE_FETCH,
        INITIALIZATION,
        EMAIL_ANALYSIS,
        MESSAGE_PARSING,
        VALIDATION,
        NEWSLETTER_ANALYSIS,
        BATCH_PROCESSING,
        METADATA_PROCESSING,
        STREAM_PROCESSING,
        FOLDER_DISCOVERY,
        FOLDER_ACCESS,
        CONNECTION_CHECK,
        MESSAGE_VALIDATION,     // Hinzugefügt
        EMAIL_EXTRACTION        // Hinzugefügt
    }

    public EmailProcessingException(String message, String emailId, String folder, ProcessingStage stage) {
        super(message);
        this.emailId = emailId;
        this.folder = folder;
        this.stage = stage;
    }

    public EmailProcessingException(String message, String emailId, String folder, ProcessingStage stage, Throwable cause) {
        super(message, cause);
        this.emailId = emailId;
        this.folder = folder;
        this.stage = stage;
    }

    public String getEmailId() {
        return emailId;
    }

    public String getFolder() {
        return folder;
    }

    public ProcessingStage getStage() {
        return stage;
    }

    public boolean isRecoverable() {
        return stage != ProcessingStage.STREAM_PROCESSING;
    }
}