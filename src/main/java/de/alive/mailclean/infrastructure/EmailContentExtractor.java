package de.alive.mailclean.infrastructure;

import lombok.extern.slf4j.Slf4j;

import javax.mail.*;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;

@Slf4j
public class EmailContentExtractor {

    public String extractSubjectSafely(MimeMessage message) {
        try {
            return message.getSubject() != null ? message.getSubject() : "";
        } catch (MessagingException e) {
            log.debug("Error extracting subject: {}", e.getMessage());
            return "";
        }
    }

    public String extractFromSafely(MimeMessage message) {
        try {
            return message.getFrom() != null && message.getFrom().length > 0
                    ? message.getFrom()[0].toString() : "";
        } catch (MessagingException e) {
            log.debug("Error extracting from: {}", e.getMessage());
            return "";
        }
    }

    public String extractFullContent(MimeMessage message, int threadId, int connId) {
        try {
            Object content = message.getContent();

            if (content instanceof String) {
                return extractTextContent(message);
            } else if (content instanceof MimeMultipart) {
                return extractMultipartContent(message, threadId, connId);
            } else {
                log.debug("Thread {} Conn {}: Unknown content type: {}",
                        threadId, connId, content.getClass().getSimpleName());
                return extractSubjectSafely(message) + " " + extractFromSafely(message);
            }

        } catch (Exception e) {
            log.debug("Thread {} Conn {}: Full content extraction error: {}",
                    threadId, connId, e.getMessage());
            return extractSubjectSafely(message) + " " + extractFromSafely(message);
        }
    }

    private String extractTextContent(MimeMessage message) {
        try {
            return message.getContent().toString();
        } catch (Exception e) {
            log.debug("Error extracting text content: {}", e.getMessage());
            return "";
        }
    }

    private String extractMultipartContent(MimeMessage message, int threadId, int connId) {
        try {
            MimeMultipart multipart = (MimeMultipart) message.getContent();
            StringBuilder contentBuilder = new StringBuilder();

            for (int i = 0; i < multipart.getCount(); i++) {
                BodyPart bodyPart = multipart.getBodyPart(i);

                if (bodyPart.isMimeType("text/plain") || bodyPart.isMimeType("text/html")) {
                    try {
                        Object partContent = bodyPart.getContent();
                        if (partContent instanceof String) {
                            contentBuilder.append(partContent).append("\n");
                        }
                    } catch (IOException | MessagingException e) {
                        log.debug("Thread {} Conn {}: Error extracting part {}: {}",
                                threadId, connId, i, e.getMessage());
                    }
                }
            }

            return contentBuilder.toString();

        } catch (Exception e) {
            log.debug("Thread {} Conn {}: Multipart extraction error: {}",
                    threadId, connId, e.getMessage());
            return "";
        }
    }
}