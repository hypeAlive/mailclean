package de.alive.mailclean.domain;

import java.util.HashMap;
import java.util.Map;

public class Email {
    private final int messageNumber;
    private final String subject;
    private final String sender;
    private final String content;
    private final String folderName;
    private final Map<String, Object> metadata;

    public Email(int messageNumber, String subject, String sender, String content, String folderName) {
        this.messageNumber = messageNumber;
        this.subject = subject != null ? subject : "";
        this.sender = sender != null ? sender : "";
        this.content = content != null ? content : "";
        this.folderName = folderName != null ? folderName : "";
        this.metadata = new HashMap<>();
    }

    public int getMessageNumber() { return messageNumber; }
    public String getSubject() { return subject; }
    public String getSender() { return sender; }
    public String getContent() { return content; }
    public String getFolderName() { return folderName; }

    public void addMetadata(String key, Object value) { metadata.put(key, value); }
    public Object getMetadata(String key) { return metadata.get(key); }
    public boolean hasMetadata(String key) { return metadata.containsKey(key); }

    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key, Class<T> type) {
        Object value = metadata.get(key);
        return type.isInstance(value) ? (T) value : null;
    }

    @Override
    public String toString() {
        return String.format("Email{#%d, subject='%s', sender='%s', folder='%s', metadata=%d}",
                messageNumber, subject, sender, folderName, metadata.size());
    }
}