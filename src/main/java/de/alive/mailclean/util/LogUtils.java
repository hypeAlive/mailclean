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
    public static final String CHART_EMOJI = "📊";
    public static final String MEMORY_EMOJI = "🧠";
    public static final String SPEED_EMOJI = "🏃";

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String formatProgress(int current, int total, long startTimeMs) {
        if (total == 0) return "[████████████████████] 0/0 (0.0%)";

        double percentage = ((double) current / total) * 100;
        int bars = (int) (percentage / 5);

        String progressBar = String.format("[%s%s] %d/%d (%.1f%%)",
                "█".repeat(Math.max(0, bars)),
                "░".repeat(Math.max(0, 20 - bars)),
                current, total, percentage);

        if (current > 0 && startTimeMs > 0) {
            String speed = formatProcessingSpeed(current, startTimeMs);
            String eta = formatETA(current, total, startTimeMs);
            return String.format("%s %s %s", progressBar, speed, eta);
        }

        return progressBar;
    }

    public static String formatProcessingSpeed(int processed, long startTimeMs) {
        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        if (elapsedMs == 0) return "";

        double emailsPerSecond = (processed * 1000.0) / elapsedMs;
        if (emailsPerSecond < 1) {
            return String.format("%s %.1f/min", SPEED_EMOJI, emailsPerSecond * 60);
        }
        return String.format("%s %.1f/s", SPEED_EMOJI, emailsPerSecond);
    }

    public static String formatETA(int current, int total, long startTimeMs) {
        if (current == 0 || total == 0) return "";

        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        long avgTimePerEmail = elapsedMs / current;
        long remainingEmails = total - current;
        long etaMs = remainingEmails * avgTimePerEmail;

        return String.format("ETA: %s", formatDuration(etaMs, false));
    }

    public static String formatDurationMs(long startTimeMs) {
        return formatDuration(System.currentTimeMillis() - startTimeMs, false);
    }

    public static String formatDuration(long durationMs, boolean precise) {
        long seconds = durationMs / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) return String.format("%dh %dm %ds", hours, minutes % 60, seconds % 60);
        if (minutes > 0) return String.format("%dm %ds", minutes, seconds % 60);
        if (precise && seconds < 10) return String.format("%.2fs", durationMs / 1000.0);
        return String.format("%ds", seconds);
    }

    public static String timestamp() {
        return LocalDateTime.now().format(TIME_FORMATTER);
    }

    public static String formatSize(int size) {
        if (size < 1000) return String.valueOf(size);
        if (size < 1_000_000) return String.format("%.1fK", size / 1000.0);
        return String.format("%.1fM", size / 1_000_000.0);
    }

    public static String formatMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        double usedMB = usedMemory / (1024.0 * 1024.0);
        double maxMB = maxMemory / (1024.0 * 1024.0);
        double usagePercent = (usedMemory * 100.0) / maxMemory;

        return String.format("%s %.0fMB/%.0fMB (%.1f%%)", MEMORY_EMOJI, usedMB, maxMB, usagePercent);
    }

    public static String formatThroughputSummary(int processed, long startTimeMs, int errors) {
        if (processed == 0) return "No items processed";

        String speed = formatProcessingSpeed(processed, startTimeMs);
        String duration = formatDurationMs(startTimeMs);
        double errorRate = (errors * 100.0) / processed;

        return String.format("%s processed in %s - Error rate: %.1f%% %s",
                formatSize(processed), duration, errorRate, speed);
    }
}