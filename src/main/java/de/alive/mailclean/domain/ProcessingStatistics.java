package de.alive.mailclean.domain;

import java.util.concurrent.atomic.AtomicInteger;

public class ProcessingStatistics {
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicInteger totalEmitted = new AtomicInteger(0);
    private final AtomicInteger totalErrors = new AtomicInteger(0);

    public void incrementProcessed() { totalProcessed.incrementAndGet(); }
    public void incrementEmitted() { totalEmitted.incrementAndGet(); }
    public void incrementErrors() { totalErrors.incrementAndGet(); }

    public int getTotalProcessed() { return totalProcessed.get(); }
    public int getTotalEmitted() { return totalEmitted.get(); }
    public int getTotalErrors() { return totalErrors.get(); }

    public void reset() {
        totalProcessed.set(0);
        totalEmitted.set(0);
        totalErrors.set(0);
    }
}