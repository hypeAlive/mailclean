package de.alive.mailclean.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class LogUtils {
    public static final String SUCCESS_EMOJI = "✅";
    public static final String ERROR_EMOJI = "❌";
    public static final String WARNING_EMOJI = "⚠️";
    public static final String INFO_EMOJI = "ℹ️";
    public static final String SEARCH_EMOJI = "🔍";
    public static final String FOLDER_EMOJI = "📁";
    public static final String EMAIL_EMOJI = "📧";
    public static final String PROCESS_EMOJI = "⚙️";
    public static final String STOP_EMOJI = "🛑";
    public static final String HEARTBEAT_EMOJI = "💓";
    public static final String ROCKET_EMOJI = "🚀";
    public static final String FIRE_EMOJI = "🔥";
    public static final String LIGHTNING_EMOJI = "⚡";

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String formatProgress(int current, int total) {
        if (total == 0) return "[████████████████████] 0/0 (0.0%)";

        double percentage = ((double) current / total) * 100;
        int bars = (int) (percentage / 5); // 20 bars total
        return String.format("[%s%s] %d/%d (%.1f%%)",
                "█".repeat(Math.max(0, bars)),
                "░".repeat(Math.max(0, 20 - bars)),
                current, total, percentage);
    }

    public static String formatDuration(long startTimeMs) {
        long durationMs = System.currentTimeMillis() - startTimeMs;
        long seconds = durationMs / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) return String.format("%dh %dm %ds", hours, minutes % 60, seconds % 60);
        if (minutes > 0) return String.format("%dm %ds", minutes, seconds % 60);
        return String.format("%.2fs", durationMs / 1000.0);
    }

    public static String timestamp() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }

    public static String formatSize(int size) {
        if (size < 1000) return String.valueOf(size);
        if (size < 1_000_000) return String.format("%.1fK", size / 1000.0);
        return String.format("%.1fM", size / 1_000_000.0);
    }
}