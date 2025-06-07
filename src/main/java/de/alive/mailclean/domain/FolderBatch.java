package de.alive.mailclean.domain;

import java.time.LocalDateTime;
import java.util.UUID;

public record FolderBatch(
        String id,
        String folderName,
        int start,
        int end,
        LocalDateTime createdAt,
        int priority
) {

    public FolderBatch(String folderName, int start, int end) {
        this(
                generateBatchId(folderName, start, end),
                folderName,
                start,
                end,
                LocalDateTime.now(),
                calculatePriority(folderName, end - start + 1)
        );
    }

    public FolderBatch {
        if (folderName == null || folderName.trim().isEmpty()) {
            throw new IllegalArgumentException("Folder name cannot be null or empty");
        }
        if (start <= 0 || end <= 0 || start > end) {
            throw new IllegalArgumentException("Invalid batch range: start=" + start + ", end=" + end);
        }
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Batch ID cannot be null or empty");
        }
    }

    private static String generateBatchId(String folderName, int start, int end) {
        String cleanFolderName = folderName.replaceAll("[^a-zA-Z0-9]", "");
        return String.format("%s-%d-%d-%s",
                cleanFolderName, start, end,
                UUID.randomUUID().toString().substring(0, 8));
    }

    private static int calculatePriority(String folderName, int batchSize) {
        if ("INBOX".equalsIgnoreCase(folderName)) {
            return 1; // Highest priority
        }
        if (folderName.toLowerCase().contains("sent")) {
            return 3; // Lower priority for sent items
        }
        if (batchSize > 1000) {
            return 2; // Medium priority for large batches
        }
        return 2; // Default priority
    }

    public int size() {
        return end - start + 1;
    }

    public boolean isLargeBatch() {
        return size() > 500;
    }

    public boolean isHighPriority() {
        return priority == 1;
    }

    public boolean isInboxBatch() {
        return "INBOX".equalsIgnoreCase(folderName);
    }

    public double getEstimatedProcessingTimeSeconds() {
        return size() * 0.05; // Estimate 50ms per email
    }

    public String getDisplayInfo() {
        return String.format("Batch[%s]: %s (%d-%d) = %d emails, priority=%d",
                id.substring(0, 8), folderName, start, end, size(), priority);
    }

    public FolderBatch withAdjustedEnd(int newEnd) {
        if (newEnd < start) {
            throw new IllegalArgumentException("New end cannot be less than start");
        }
        return new FolderBatch(folderName, start, newEnd);
    }

    public boolean overlaps(FolderBatch other) {
        if (!folderName.equals(other.folderName)) {
            return false;
        }
        return start <= other.end && end >= other.start;
    }

    public int getProgressPercentage(int currentMessage) {
        if (currentMessage < start) return 0;
        if (currentMessage > end) return 100;

        int processed = currentMessage - start + 1;
        return (processed * 100) / size();
    }
}