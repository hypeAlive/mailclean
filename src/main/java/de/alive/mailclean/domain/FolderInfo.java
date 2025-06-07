package de.alive.mailclean.domain;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.IntStream;

public record FolderInfo(
        String folderName,
        int messageCount,
        LocalDateTime discoveredAt,
        FolderType type,
        int estimatedBatches
) {

    public FolderInfo(String folderName, int messageCount) {
        this(
                folderName,
                messageCount,
                LocalDateTime.now(),
                determineFolderType(folderName),
                calculateBatchCount(messageCount)
        );
    }

    public FolderInfo {
        if (folderName == null || folderName.trim().isEmpty()) {
            throw new IllegalArgumentException("Folder name cannot be null or empty");
        }
        if (messageCount < 0) {
            throw new IllegalArgumentException("Message count cannot be negative");
        }
        if (estimatedBatches < 0) {
            throw new IllegalArgumentException("Estimated batches cannot be negative");
        }
    }

    private static FolderType determineFolderType(String folderName) {
        String lower = folderName.toLowerCase();

        if ("inbox".equals(lower)) return FolderType.INBOX;
        if (lower.contains("sent")) return FolderType.SENT;
        if (lower.contains("draft")) return FolderType.DRAFTS;
        if (lower.contains("spam") || lower.contains("junk")) return FolderType.SPAM;
        if (lower.contains("trash") || lower.contains("deleted")) return FolderType.TRASH;
        if (lower.contains("archive")) return FolderType.ARCHIVE;

        return FolderType.OTHER;
    }

    private static int calculateBatchCount(int messageCount) {
        if (messageCount == 0) return 0;
        return (int) Math.ceil(messageCount / 500.0); // 500 messages per batch
    }

    public boolean hasMessages() {
        return messageCount > 0;
    }

    public boolean isHighVolume() {
        return messageCount > 10000;
    }

    public boolean isLowVolume() {
        return messageCount > 0 && messageCount <= 100;
    }

    public int getPriority() {
        return switch (type) {
            case INBOX -> 1;
            case SENT -> 3;
            case SPAM, TRASH -> 4;
            case DRAFTS, ARCHIVE -> 3;
            case OTHER -> 2;
        };
    }

    public double getEstimatedProcessingTimeMinutes() {
        return (messageCount * 0.05) / 60.0; // 50ms per email, converted to minutes
    }

    public List<FolderBatch> createBatches(int batchSize) {
        if (!hasMessages()) {
            return List.of();
        }

        return IntStream.rangeClosed(1, messageCount)
                .filter(i -> (i - 1) % batchSize == 0)
                .mapToObj(start -> {
                    int end = Math.min(start + batchSize - 1, messageCount);
                    return new FolderBatch(folderName, start, end);
                })
                .toList();
    }

    public String getDisplayInfo() {
        return String.format("Folder[%s]: %,d messages (%s, priority=%d, ~%.1f min)",
                folderName, messageCount, type, getPriority(), getEstimatedProcessingTimeMinutes());
    }

    public enum FolderType {
        INBOX("Inbox"),
        SENT("Sent Items"),
        DRAFTS("Drafts"),
        SPAM("Spam/Junk"),
        TRASH("Trash/Deleted"),
        ARCHIVE("Archive"),
        OTHER("Other");

        private final String displayName;

        FolderType(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String toString() {
            return displayName;
        }
    }
}