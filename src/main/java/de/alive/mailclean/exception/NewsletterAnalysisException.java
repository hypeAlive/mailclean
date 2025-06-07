package de.alive.mailclean.exception;

public class NewsletterAnalysisException extends Exception {

    private final String emailSubject;
    private final String sender;
    private final AnalysisStage stage;

    public enum AnalysisStage {
        PATTERN_DETECTION,
        LINK_EXTRACTION,
        SENDER_ANALYSIS,
        CONTENT_PARSING
    }

    public NewsletterAnalysisException(String message, String emailSubject, String sender, AnalysisStage stage) {
        super(message);
        this.emailSubject = emailSubject;
        this.sender = sender;
        this.stage = stage;
    }

    public NewsletterAnalysisException(String message, String emailSubject, String sender, AnalysisStage stage, Throwable cause) {
        super(message, cause);
        this.emailSubject = emailSubject;
        this.sender = sender;
        this.stage = stage;
    }

    public String getEmailSubject() {
        return emailSubject;
    }

    public String getSender() {
        return sender;
    }

    public AnalysisStage getStage() {
        return stage;
    }

    public boolean canSkipEmail() {
        return stage == AnalysisStage.PATTERN_DETECTION || stage == AnalysisStage.LINK_EXTRACTION;
    }
}