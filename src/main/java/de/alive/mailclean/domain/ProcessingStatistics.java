package de.alive.mailclean.domain;

import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class ProcessingStatistics {

    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalEmitted = new LongAdder();
    private final LongAdder totalErrors = new LongAdder();
    private final LongAdder totalSkipped = new LongAdder();
    private final LongAdder totalNewsletters = new LongAdder();

    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger activeBatches = new AtomicInteger(0);
    private final AtomicInteger completedBatches = new AtomicInteger(0);

    private final ConcurrentHashMap<Integer, ThreadStatistics> threadStats = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FolderStatistics> folderStats = new ConcurrentHashMap<>();

    private volatile int expectedTotal = 0;
    private volatile LocalDateTime lastUpdate = LocalDateTime.now();

    // Basic counters
    public void incrementProcessed() {
        totalProcessed.increment();
        updateLastActivity();
    }

    public void incrementEmitted() {
        totalEmitted.increment();
        updateLastActivity();
    }

    public void incrementErrors() {
        totalErrors.increment();
        updateLastActivity();
    }

    public void incrementSkipped() {
        totalSkipped.increment();
        updateLastActivity();
    }

    public void incrementNewsletters() {
        totalNewsletters.increment();
        updateLastActivity();
    }

    // Batch tracking
    public void batchStarted() {
        activeBatches.incrementAndGet();
        updateLastActivity();
    }

    public void batchCompleted() {
        activeBatches.decrementAndGet();
        completedBatches.incrementAndGet();
        updateLastActivity();
    }

    // Thread tracking
    public void threadStarted(int threadId) {
        activeThreads.incrementAndGet();
        threadStats.put(threadId, new ThreadStatistics(threadId));
        updateLastActivity();
        log.debug("{} Thread {} started - {} threads active",
                LogUtils.PROCESS_EMOJI, threadId, activeThreads.get());
    }

    public void threadCompleted(int threadId) {
        activeThreads.decrementAndGet();
        ThreadStatistics stats = threadStats.get(threadId);
        if (stats != null) {
            stats.setEndTime(System.currentTimeMillis());
        }
        updateLastActivity();
        log.debug("{} Thread {} completed - {} threads remaining",
                LogUtils.SUCCESS_EMOJI, threadId, activeThreads.get());
    }

    public void recordThreadActivity(int threadId, String activity, int count) {
        ThreadStatistics stats = threadStats.get(threadId);
        if (stats != null) {
            stats.recordActivity(activity, count);
        }
        updateLastActivity();
    }

    // Folder tracking
    public void recordFolderActivity(String folderName, int processed, int errors) {
        folderStats.computeIfAbsent(folderName, FolderStatistics::new)
                .recordActivity(processed, errors);
        updateLastActivity();
    }

    // Getters
    public long getTotalProcessed() { return totalProcessed.sum(); }
    public long getTotalEmitted() { return totalEmitted.sum(); }
    public long getTotalErrors() { return totalErrors.sum(); }
    public long getTotalSkipped() { return totalSkipped.sum(); }
    public long getTotalNewsletters() { return totalNewsletters.sum(); }

    public int getActiveThreads() { return activeThreads.get(); }
    public int getActiveBatches() { return activeBatches.get(); }
    public int getCompletedBatches() { return completedBatches.get(); }

    public void setExpectedTotal(int total) {
        this.expectedTotal = total;
        updateLastActivity();
    }

    public int getExpectedTotal() { return expectedTotal; }

    // Performance metrics
    public double getProcessingRate() {
        long elapsed = getElapsedTimeMs();
        return elapsed > 0 ? (getTotalProcessed() * 1000.0) / elapsed : 0.0;
    }

    public double getErrorRate() {
        long total = getTotalProcessed();
        return total > 0 ? (getTotalErrors() * 100.0) / total : 0.0;
    }

    public double getNewsletterRate() {
        long total = getTotalProcessed();
        return total > 0 ? (getTotalNewsletters() * 100.0) / total : 0.0;
    }

    public double getProgressPercentage() {
        return expectedTotal > 0 ? (getTotalProcessed() * 100.0) / expectedTotal : 0.0;
    }

    public long getElapsedTimeMs() {
        return System.currentTimeMillis() - startTime.get();
    }

    public long getEstimatedRemainingTimeMs() {
        double rate = getProcessingRate();
        if (rate <= 0 || expectedTotal <= 0) return -1;

        long remaining = expectedTotal - getTotalProcessed();
        return (long) (remaining / rate * 1000);
    }

    // Status reporting
    public String getQuickStatus() {
        return String.format("Processed: %,d/%,d (%.1f%%) | Rate: %.1f/s | Errors: %,d (%.1f%%) | Time: %s",
                getTotalProcessed(), expectedTotal, getProgressPercentage(),
                getProcessingRate(), getTotalErrors(), getErrorRate(),
                LogUtils.formatDurationMs(startTime.get()));
    }

    public String getDetailedStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append(LogUtils.CHART_EMOJI).append(" PROCESSING STATISTICS:\n");
        sb.append("═══════════════════════════════════════════════════\n");

        // Overall progress
        sb.append(String.format("📊 Progress: %,d/%,d emails (%.1f%%)\n",
                getTotalProcessed(), expectedTotal, getProgressPercentage()));
        sb.append(String.format("⚡ Processing Rate: %.1f emails/second\n", getProcessingRate()));
        sb.append(String.format("📨 Emitted: %,d | Skipped: %,d | Newsletters: %,d\n",
                getTotalEmitted(), getTotalSkipped(), getTotalNewsletters()));
        sb.append(String.format("❌ Errors: %,d (%.1f%%)\n", getTotalErrors(), getErrorRate()));

        // Time information
        sb.append(String.format("⏱️ Elapsed: %s", LogUtils.formatDurationMs(startTime.get())));
        long remaining = getEstimatedRemainingTimeMs();
        if (remaining > 0) {
            sb.append(String.format(" | ETA: %s", LogUtils.formatDurationMs(remaining)));
        }
        sb.append("\n");

        // Thread information
        sb.append(String.format("🧵 Threads: %d active | Batches: %d active, %d completed\n",
                getActiveThreads(), getActiveBatches(), getCompletedBatches()));

        sb.append("═══════════════════════════════════════════════════");

        return sb.toString();
    }

    public ThreadStatistics getThreadStatistics(int threadId) {
        return threadStats.get(threadId);
    }

    public FolderStatistics getFolderStatistics(String folderName) {
        return folderStats.get(folderName);
    }

    private void updateLastActivity() {
        lastUpdate = LocalDateTime.now();
    }

    public LocalDateTime getLastUpdate() {
        return lastUpdate;
    }

    public boolean isProcessingComplete() {
        return expectedTotal > 0 && getTotalProcessed() >= expectedTotal && getActiveThreads() == 0;
    }

    public void reset() {
        startTime.set(System.currentTimeMillis());
        totalProcessed.reset();
        totalEmitted.reset();
        totalErrors.reset();
        totalSkipped.reset();
        totalNewsletters.reset();
        activeThreads.set(0);
        activeBatches.set(0);
        completedBatches.set(0);
        threadStats.clear();
        folderStats.clear();
        expectedTotal = 0;
        updateLastActivity();
    }

    // Inner classes for detailed tracking
    public static class ThreadStatistics {
        private final int threadId;
        private final long startTime;
        private volatile long endTime = -1;
        private final ConcurrentHashMap<String, AtomicInteger> activities = new ConcurrentHashMap<>();

        public ThreadStatistics(int threadId) {
            this.threadId = threadId;
            this.startTime = System.currentTimeMillis();
        }

        public void recordActivity(String activity, int count) {
            activities.computeIfAbsent(activity, k -> new AtomicInteger(0)).addAndGet(count);
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public long getDuration() {
            long end = endTime > 0 ? endTime : System.currentTimeMillis();
            return end - startTime;
        }

        public int getThreadId() { return threadId; }
        public long getStartTime() { return startTime; }
        public boolean isActive() { return endTime == -1; }

        public int getActivityCount(String activity) {
            AtomicInteger counter = activities.get(activity);
            return counter != null ? counter.get() : 0;
        }
    }

    public static class FolderStatistics {
        private final String folderName;
        private final LongAdder processed = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final long startTime = System.currentTimeMillis();

        public FolderStatistics(String folderName) {
            this.folderName = folderName;
        }

        public void recordActivity(int processedCount, int errorCount) {
            processed.add(processedCount);
            errors.add(errorCount);
        }

        public String getFolderName() { return folderName; }
        public long getProcessed() { return processed.sum(); }
        public long getErrors() { return errors.sum(); }
        public long getDuration() { return System.currentTimeMillis() - startTime; }

        public double getErrorRate() {
            long total = getProcessed();
            return total > 0 ? (getErrors() * 100.0) / total : 0.0;
        }
    }
}