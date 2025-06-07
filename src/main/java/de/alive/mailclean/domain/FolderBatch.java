package de.alive.mailclean.domain;

public record FolderBatch(String folderName, int start, int end) {

    public FolderBatch {
        if (folderName == null || folderName.trim().isEmpty()) {
            throw new IllegalArgumentException("Folder name cannot be null or empty");
        }
        if (start <= 0 || end <= 0 || start > end) {
            throw new IllegalArgumentException("Invalid batch range: start=" + start + ", end=" + end);
        }
    }

    public int size() {
        return end - start + 1;
    }
}