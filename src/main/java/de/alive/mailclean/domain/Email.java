package de.alive.mailclean.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Email {
    private final String id;
    private final int messageNumber;
    private final String subject;
    private final String sender;
    private final String content;
    private final String folder;
    private final Map<String, Object> metadata;
    private final long createdAt;

    public Email(int messageNumber, String subject, String sender, String content, String folderName) {
        this.id = generateEmailId(messageNumber, folderName);
        this.messageNumber = messageNumber;
        this.subject = sanitizeString(subject);
        this.sender = sanitizeString(sender);
        this.content = content != null ? content : "";
        this.folder = sanitizeString(folderName);
        this.metadata = new HashMap<>();
        this.createdAt = System.currentTimeMillis();
    }

    private String generateEmailId(int messageNumber, String folderName) {
        return String.format("%s-%d-%s",
                folderName != null ? folderName.replaceAll("[^a-zA-Z0-9]", "") : "unknown",
                messageNumber,
                UUID.randomUUID().toString().substring(0, 8));
    }

    private String sanitizeString(String input) {
        return input != null ? input.trim() : "";
    }

    // Getters
    public String getId() { return id; }
    public int getMessageNumber() { return messageNumber; }
    public String getSubject() { return subject; }
    public String getSender() { return sender; }
    public String getContent() { return content; }
    public String getFolder() { return folder; }
    public long getCreatedAt() { return createdAt; }

    // Enhanced metadata methods
    public void addMetadata(String key, Object value) {
        if (key != null && value != null) {
            metadata.put(key, value);
        }
    }

    public Optional<Object> getMetadata(String key) {
        return Optional.ofNullable(metadata.get(key));
    }

    public boolean hasMetadata(String key) {
        return metadata.containsKey(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key, Class<T> type) {
        Object value = metadata.get(key);
        return type.isInstance(value) ? (T) value : null;
    }

    public <T> Optional<T> getMetadataOptional(String key, Class<T> type) {
        return Optional.ofNullable(getMetadata(key, type));
    }

    public Map<String, Object> getAllMetadata() {
        return new HashMap<>(metadata);
    }

    public int getMetadataCount() {
        return metadata.size();
    }

    // Business logic helpers
    public boolean isNewsletter() {
        return getMetadata("newsletter", Boolean.class) == Boolean.TRUE;
    }

    public boolean wasSkipped() {
        return getMetadata("skipped", Boolean.class) == Boolean.TRUE;
    }

    public boolean isCacheHit() {
        return getMetadata("cache_hit", Boolean.class) == Boolean.TRUE;
    }

    public String getNormalizedSender() {
        return getMetadata("normalized_sender", String.class);
    }

    public int getContentLength() {
        return content.length();
    }

    public boolean hasContent() {
        return !content.trim().isEmpty();
    }

    public boolean hasSubject() {
        return !subject.trim().isEmpty();
    }

    @Override
    public String toString() {
        return String.format("Email{id='%s', #%d, subject='%s', sender='%s', folder='%s', metadata=%d, content=%d chars}",
                id, messageNumber,
                truncateForDisplay(subject, 30),
                truncateForDisplay(sender, 30),
                folder, metadata.size(), content.length());
    }

    private String truncateForDisplay(String text, int maxLength) {
        return text.length() > maxLength ? text.substring(0, maxLength) + "..." : text;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Email email = (Email) obj;
        return id.equals(email.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}