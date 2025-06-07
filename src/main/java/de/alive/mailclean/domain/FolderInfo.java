package de.alive.mailclean.domain;

public record FolderInfo(String folderName, int messageCount) {

    public FolderInfo {
        if (folderName == null || folderName.trim().isEmpty()) {
            throw new IllegalArgumentException("Folder name cannot be null or empty");
        }
        if (messageCount < 0) {
            throw new IllegalArgumentException("Message count cannot be negative");
        }
    }

    public boolean hasMessages() {
        return messageCount > 0;
    }
}