package de.alive.mailclean.service;

import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.util.LogUtils;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ProgressTrackingService {

    private final ProcessingStatistics statistics;
    private final AtomicInteger currentProgress = new AtomicInteger(0);
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong lastProgressUpdate = new AtomicLong();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile int totalItems = 0;
    private volatile int estimatedBatches = 0;
    private volatile boolean isRunning = false;

    // Konstruktor mit ProcessingStatistics
    public ProgressTrackingService(ProcessingStatistics statistics) {
        this.statistics = statistics;
    }

    // Standard-Konstruktor für Rückwärtskompatibilität
    public ProgressTrackingService() {
        this.statistics = null;
    }

    // Neue Methode: initializeProgress
    public void initializeProgress(int totalEmails, int estimatedBatches) {
        this.totalItems = totalEmails;
        this.estimatedBatches = estimatedBatches;
        this.lastProgressUpdate.set(System.currentTimeMillis());

        if (statistics != null) {
            statistics.setExpectedTotal(totalEmails);
        }

        startTracking(totalEmails);

        log.info("{} Progress tracking initialized: {:,} emails in ~{} batches",
                LogUtils.CHART_EMOJI, totalEmails, estimatedBatches);
    }

    // Neue Methode: reportProgress
    public void reportProgress() {
        if (!isRunning) return;

        int current = getCurrentProgressFromStatistics();
        updateProgress(current);
        lastProgressUpdate.set(System.currentTimeMillis());

        // Sofortiger Progress-Log bei manuellem Report
        logCurrentProgress();
    }

    // Neue Methode: isProcessingStalled
    public boolean isProcessingStalled() {
        long timeSinceLastUpdate = System.currentTimeMillis() - lastProgressUpdate.get();
        return timeSinceLastUpdate > 120000; // 2 Minuten
    }

    private int getCurrentProgressFromStatistics() {
        if (statistics != null) {
            return (int) statistics.getTotalProcessed();
        }
        return currentProgress.get();
    }

    private void logCurrentProgress() {
        int current = currentProgress.get();
        if (current == 0) return;

        String progressBar = LogUtils.formatProgress(current, totalItems, startTime.get());
        String memoryInfo = LogUtils.formatMemoryUsage();
        double percentage = getCompletionPercentage();

        log.info("{} Progress: {:.1f}% ({:,}/{:,}) {} {}",
                LogUtils.CHART_EMOJI, percentage, current, totalItems, progressBar, memoryInfo);
    }


    // Bestehende Methoden bleiben unverändert...
    public void startTracking(int totalItems) {
        this.totalItems = totalItems;
        this.startTime.set(System.currentTimeMillis());
        this.currentProgress.set(0);
        this.isRunning = true;

        log.info("{} Starting progress tracking for {} items", LogUtils.ROCKET_EMOJI, LogUtils.formatSize(totalItems));

        scheduler.scheduleAtFixedRate(this::logProgress, 30, 30, TimeUnit.SECONDS);
    }

    public void updateProgress(int current) {
        currentProgress.set(current);
    }

    public void stopTracking() {
        isRunning = false;
        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logFinalProgress();
    }

    private void logProgress() {
        if (!isRunning) return;

        int current = currentProgress.get();
        if (current == 0) return;

        String progressBar = LogUtils.formatProgress(current, totalItems, startTime.get());
        String memoryInfo = LogUtils.formatMemoryUsage();

        log.info("{} Progress: {} {}", LogUtils.CHART_EMOJI, progressBar, memoryInfo);

        if (current >= totalItems) {
            isRunning = false;
        }
    }

    private void logFinalProgress() {
        int final_count = currentProgress.get();
        String summary = LogUtils.formatThroughputSummary(final_count, startTime.get(), 0);

        log.info("{} Progress tracking completed: {}", LogUtils.SUCCESS_EMOJI, summary);
    }

    public double getCompletionPercentage() {
        if (totalItems == 0) return 0.0;
        return (currentProgress.get() * 100.0) / totalItems;
    }

    public long getElapsedTimeMs() {
        return System.currentTimeMillis() - startTime.get();
    }

    public int getCurrentProgress() {
        return currentProgress.get();
    }

    public int getTotalItems() {
        return totalItems;
    }
}