package de.alive.mailclean.domain;

import de.alive.mailclean.util.LogUtils;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessingStatistics implements ProcessingTracker {
    private final AtomicInteger currentProgress = new AtomicInteger(0);
    private final int totalProgress;

    public ProcessingStatistics(int totalProgress) throws IllegalArgumentException {
        if (totalProgress < 0)
            throw new IllegalArgumentException("Total progress cannot be negative");
        this.totalProgress = totalProgress;
    }

    public String formattedProgress() {
        return LogUtils.formatProgress(currentProgress.get(), totalProgress);
    }

    public void incrementProgress() {
        currentProgress.incrementAndGet();
    }

    public int currentProgress() {
        return currentProgress.get();
    }

}