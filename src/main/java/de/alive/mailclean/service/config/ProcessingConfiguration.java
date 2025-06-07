package de.alive.mailclean.service.config;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;

@Data
@Builder(toBuilder = true)
public class ProcessingConfiguration {

    // Thread configuration
    private final int threadCount;
    private final int batchSize;
    private final int streamBufferSize;

    // Connection configuration
    private final int maxConcurrentConnections;
    private final Duration connectionTimeout;
    private final Duration processingTimeout;

    // Performance settings
    private final boolean enableVerboseLogging;
    private final double maxErrorRate;

    // Progress reporting settings
    private final int progressReportInterval;
    private final long streamTimeoutMs;

    // Default values
    public static final int DEFAULT_THREAD_COUNT = 4;
    public static final int DEFAULT_BATCH_SIZE = 250;
    public static final int DEFAULT_STREAM_BUFFER_SIZE = 1000;
    public static final int DEFAULT_MAX_CONNECTIONS = 16;
    public static final double DEFAULT_MAX_ERROR_RATE = 0.1; // 10%
    public static final int DEFAULT_PROGRESS_REPORT_INTERVAL = 100;
    public static final long DEFAULT_STREAM_TIMEOUT_MS = 30000; // 30 seconds

    // Environment-specific configurations
    public static ProcessingConfiguration forDevelopment() {
        return ProcessingConfiguration.builder()
                .threadCount(2)
                .batchSize(100)
                .streamBufferSize(500)
                .maxConcurrentConnections(8)
                .connectionTimeout(Duration.ofSeconds(30))
                .processingTimeout(Duration.ofMinutes(2))
                .enableVerboseLogging(true)
                .maxErrorRate(DEFAULT_MAX_ERROR_RATE)
                .progressReportInterval(50)
                .streamTimeoutMs(15000)
                .build();
    }

    public static ProcessingConfiguration forProduction() {
        return ProcessingConfiguration.builder()
                .threadCount(Math.min(6, calculateOptimalThreadCount())) // Begrenzt auf max 6
                .batchSize(250)  // Kleinere Batches für bessere Parallelisierung
                .streamBufferSize(1000)
                .maxConcurrentConnections(16) // Mehr Connections verfügbar machen
                .connectionTimeout(Duration.ofSeconds(30))
                .processingTimeout(Duration.ofMinutes(3))
                .enableVerboseLogging(false)
                .maxErrorRate(DEFAULT_MAX_ERROR_RATE)
                .progressReportInterval(DEFAULT_PROGRESS_REPORT_INTERVAL)
                .streamTimeoutMs(DEFAULT_STREAM_TIMEOUT_MS)
                .build();
    }

    public static ProcessingConfiguration forTesting() {
        return ProcessingConfiguration.builder()
                .threadCount(1)
                .batchSize(50)
                .streamBufferSize(200)
                .maxConcurrentConnections(4)
                .connectionTimeout(Duration.ofSeconds(15))
                .processingTimeout(Duration.ofMinutes(1))
                .enableVerboseLogging(true)
                .maxErrorRate(DEFAULT_MAX_ERROR_RATE)
                .progressReportInterval(25)
                .streamTimeoutMs(10000)
                .build();
    }

    private static int calculateOptimalThreadCount() {
        int cores = Runtime.getRuntime().availableProcessors();
        // I/O-intensive Arbeit: mehr Threads als Cores, aber begrenzt
        int optimal = cores * 2;
        return Math.min(optimal, 8); // Maximum 8 Threads
    }

    // Validation methods
    public void validate() {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("Thread count must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
        if (streamBufferSize <= 0) {
            throw new IllegalArgumentException("Stream buffer size must be positive");
        }
        if (maxConcurrentConnections <= 0) {
            throw new IllegalArgumentException("Max concurrent connections must be positive");
        }
        if (maxConcurrentConnections < threadCount * 2) {
            throw new IllegalArgumentException("Max connections should be at least 2x thread count");
        }
        if (maxErrorRate < 0.0 || maxErrorRate > 1.0) {
            throw new IllegalArgumentException("Max error rate must be between 0.0 and 1.0");
        }
        if (progressReportInterval <= 0) {
            throw new IllegalArgumentException("Progress report interval must be positive");
        }
        if (streamTimeoutMs <= 0) {
            throw new IllegalArgumentException("Stream timeout must be positive");
        }
    }

    // Utility methods
    public boolean isHighThroughputMode() {
        return threadCount >= 6 && batchSize >= 500;
    }

    public boolean isLowLatencyMode() {
        return threadCount <= 2 && batchSize <= 100;
    }

    public String getConfigurationSummary() {
        return String.format(
                "Threads: %d, Batch: %d, Buffer: %d, MaxConn: %d, Timeout: %ds, Progress: %d",
                threadCount, batchSize, streamBufferSize,
                maxConcurrentConnections, connectionTimeout.toSeconds(), progressReportInterval
        );
    }

    // Alias für getSummary() - für Kompatibilität
    public String getSummary() {
        return getConfigurationSummary();
    }

    // Alias für enableDetailedLogging - für Builder-Kompatibilität
    public static class ProcessingConfigurationBuilder {
        public ProcessingConfigurationBuilder enableDetailedLogging(boolean enabled) {
            return this.enableVerboseLogging(enabled);
        }
    }
}