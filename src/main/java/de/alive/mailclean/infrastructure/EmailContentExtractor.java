package de.alive.mailclean.infrastructure;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.Optional;

@Slf4j
public class EmailContentExtractor {

    private static final int MAX_CONTENT_LENGTH = 500_000; // 500KB limit
    private static final int MAX_SUBJECT_LENGTH = 1000;
    private static final int MAX_SENDER_LENGTH = 500;

    public Email extractEmail(Message message, String folderName, int threadId, int connectionId)
            throws EmailProcessingException {

        if (message == null) {
            throw new EmailProcessingException(
                    "Message cannot be null",
                    "unknown",
                    folderName,
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Null message")
            );
        }

        try {
            MimeMessage mimeMessage = (MimeMessage) message;
            int messageNumber = message.getMessageNumber();

            // Extrahiere alle benötigten Daten
            String subject = extractSubject(mimeMessage, messageNumber).orElse("");
            String sender = extractSender(mimeMessage, messageNumber).orElse("");
            String content = extractContent(mimeMessage, messageNumber);

            // Erstelle Email mit allen erforderlichen Parametern
            Email email = new Email(messageNumber, subject, sender, content, folderName);

            // Füge Metadaten hinzu
            email.addMetadata("thread_id", threadId);
            email.addMetadata("connection_id", connectionId);
            email.addMetadata("extraction_time", System.currentTimeMillis());

            return email;

        } catch (Exception e) {
            String emailId = folderName + "-" + message.getMessageNumber();
            log.error("{} Failed to extract email {}: {}",
                    LogUtils.ERROR_EMOJI, emailId, e.getMessage());

            throw new EmailProcessingException(
                    "Email extraction failed: " + e.getMessage(),
                    emailId,
                    folderName,
                    EmailProcessingException.ProcessingStage.CONTENT_EXTRACTION,
                    e
            );
        }
    }

    private String extractContent(MimeMessage mimeMessage, int messageNumber) {
        try {
            String content = extractContentSafely(mimeMessage);

            if (content != null && !content.trim().isEmpty()) {
                // Begrenze Content-Länge
                if (content.length() > MAX_CONTENT_LENGTH) {
                    content = content.substring(0, MAX_CONTENT_LENGTH) + "... [TRUNCATED]";
                }
                return content;
            } else {
                return buildFallbackContent(mimeMessage, messageNumber);
            }

        } catch (Exception e) {
            log.debug("Content extraction failed for message {}, using fallback: {}",
                    messageNumber, e.getMessage());
            return buildFallbackContent(mimeMessage, messageNumber);
        }
    }

    private String extractContentSafely(MimeMessage mimeMessage) {
        try {
            Object content = mimeMessage.getContent();
            if (content instanceof String) {
                return (String) content;
            } else {
                return content.toString();
            }
        } catch (IOException | MessagingException e) {
            log.debug("Failed to extract content: {}", e.getMessage());
            return null;
        }
    }

    private String buildFallbackContent(MimeMessage mimeMessage, int messageNumber) {
        try {
            StringBuilder fallback = new StringBuilder();
            fallback.append("Subject: ")
                    .append(extractSubject(mimeMessage, messageNumber).orElse("No Subject"))
                    .append("\n");
            fallback.append("From: ")
                    .append(extractSender(mimeMessage, messageNumber).orElse("Unknown Sender"))
                    .append("\n");
            fallback.append("Message Number: ").append(messageNumber).append("\n");
            fallback.append("Content extraction failed - using metadata only");

            return fallback.toString();
        } catch (Exception e) {
            return String.format("Email #%d - Content extraction failed: %s",
                    messageNumber, e.getMessage());
        }
    }

    private Optional<String> extractSubject(MimeMessage mimeMessage, int messageNumber) {
        try {
            String subject = mimeMessage.getSubject();
            if (subject != null && !subject.trim().isEmpty()) {
                // Begrenze Subject-Länge
                if (subject.length() > MAX_SUBJECT_LENGTH) {
                    subject = subject.substring(0, MAX_SUBJECT_LENGTH) + "...";
                }
                return Optional.of(subject.trim());
            }
            return Optional.empty();
        } catch (MessagingException e) {
            log.debug("Failed to extract subject for message {}: {}", messageNumber, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<String> extractSender(MimeMessage mimeMessage, int messageNumber) {
        try {
            String sender = null;

            // Versuche verschiedene Sender-Felder
            if (mimeMessage.getFrom() != null && mimeMessage.getFrom().length > 0) {
                sender = mimeMessage.getFrom()[0].toString();
            } else if (mimeMessage.getSender() != null) {
                sender = mimeMessage.getSender().toString();
            }

            if (sender != null && !sender.trim().isEmpty()) {
                // Begrenze Sender-Länge
                if (sender.length() > MAX_SENDER_LENGTH) {
                    sender = sender.substring(0, MAX_SENDER_LENGTH) + "...";
                }
                return Optional.of(sender.trim());
            }
            return Optional.empty();

        } catch (MessagingException e) {
            log.debug("Failed to extract sender for message {}: {}", messageNumber, e.getMessage());
            return Optional.empty();
        }
    }

    public boolean canExtract(Message message) {
        try {
            return message != null &&
                    message instanceof MimeMessage &&
                    message.getMessageNumber() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public String getExtractorInfo() {
        return String.format("EmailContentExtractor{maxContent=%d, maxSubject=%d, maxSender=%d}",
                MAX_CONTENT_LENGTH, MAX_SUBJECT_LENGTH, MAX_SENDER_LENGTH);
    }
}