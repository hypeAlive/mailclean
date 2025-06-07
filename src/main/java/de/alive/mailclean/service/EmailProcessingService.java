package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.FolderInfo;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.service.config.ProcessingConfiguration;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;  // Hinzugefügter Import
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main service for orchestrating email processing operations.
 * Coordinates batch processing, statistics tracking, and progress reporting.
 */
@Slf4j
public class EmailProcessingService {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(30);
    private static final int PROGRESS_UPDATE_INTERVAL = 1000; // milliseconds

    private final ImapConnectionManager connectionManager;
    private final EmailAnalysisService analysisService;
    private final ProcessingConfiguration config;
    private final ProcessingStatistics statistics;
    private final Sinks.Many<Email> emailQueue;
    private final BatchProcessingService batchProcessingService;
    private final ProgressTracker progressTracker;
    private final FolderDiscoveryService folderDiscoveryService;

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private ExecutorService processingExecutor;
    private ScheduledExecutorService progressReporter;

    public EmailProcessingService(ImapConnectionManager connectionManager,
                                  EmailAnalysisService analysisService,
                                  ProcessingConfiguration config,
                                  FolderDiscoveryService folderDiscoveryService) {
        this.connectionManager = connectionManager;
        this.analysisService = analysisService;
        this.config = config;
        this.folderDiscoveryService = folderDiscoveryService;
        this.statistics = new ProcessingStatistics();
        this.emailQueue = Sinks.many().multicast().onBackpressureBuffer();

        this.batchProcessingService = new BatchProcessingService(
                connectionManager,
                statistics,
                emailQueue,
                analysisService,
                config
        );

        this.processingExecutor = Executors.newFixedThreadPool(config.getThreadCount());
        this.progressTracker = new ProgressTracker();

        log.info("{} Email Processing Service initialized with configuration: {}",
                LogUtils.SUCCESS_EMOJI, config.getConfigurationSummary());
    }

    /**
     * Processes emails in specified folder names.
     * This method discovers the folders first, then processes them.
     */
    public CompletableFuture<ProcessingResult> processAllEmailsAsync(List<String> folderNames) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Discover folders from names
                List<FolderInfo> folderInfos = folderDiscoveryService.discoverFoldersFromNames(folderNames);
                return processAllFolders(folderInfos);
            } catch (Exception e) {
                throw new RuntimeException("Failed to process emails in folders: " + folderNames, e);
            }
        }, processingExecutor);
    }

    /**
     * Processes all emails in the specified folders.
     */
    public CompletableFuture<ProcessingResult> processAllFoldersAsync(List<FolderInfo> folderInfos) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return processAllFolders(folderInfos);
            } catch (Exception e) {
                throw new RuntimeException("Failed to process folders", e);
            }
        }, processingExecutor);
    }

    /**
     * Synchronous processing of all folders.
     */
    public ProcessingResult processAllFolders(List<FolderInfo> folderInfos)
            throws EmailProcessingException, InterruptedException {

        if (!isProcessing.compareAndSet(false, true)) {
            throw new EmailProcessingException(
                    "Processing is already in progress",
                    "CONCURRENT_PROCESSING",
                    "SYSTEM",
                    EmailProcessingException.ProcessingStage.INITIALIZATION,
                    null
            );
        }

        try {
            validateInputs(folderInfos);

            log.info("{} Starting email processing for {} folders",
                    LogUtils.ROCKET_EMOJI, folderInfos.size());

            ProcessingSession session = initializeSession(folderInfos);
            startProgressReporting();

            // Start email stream processing
            CompletableFuture<Void> streamProcessor = startEmailStreamProcessing();

            // Start batch processing
            CompletableFuture<BatchProcessingService.ProcessingResult> batchProcessor =
                    batchProcessingService.processAllFoldersAsync(folderInfos, config.getThreadCount());

            // Wait for both to complete
            CompletableFuture.allOf(streamProcessor, batchProcessor).get(
                    config.getProcessingTimeout().toSeconds(),
                    TimeUnit.SECONDS
            );

            BatchProcessingService.ProcessingResult batchResult = batchProcessor.get();

            return createFinalResult(session, batchResult);

        } catch (TimeoutException e) {
            throw new EmailProcessingException(
                    "Processing timeout exceeded",
                    "PROCESSING_TIMEOUT",
                    "SYSTEM",
                    EmailProcessingException.ProcessingStage.STREAM_PROCESSING,
                    e
            );
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof EmailProcessingException) {
                throw (EmailProcessingException) cause;
            }
            throw new EmailProcessingException(
                    "Processing execution failed: " + cause.getMessage(),
                    "EXECUTION_ERROR",
                    "SYSTEM",
                    EmailProcessingException.ProcessingStage.BATCH_PROCESSING,
                    cause
            );
        } finally {
            shutdownGracefully();
            isProcessing.set(false);
        }
    }

    private void validateInputs(List<FolderInfo> folderInfos) throws EmailProcessingException {
        if (folderInfos == null || folderInfos.isEmpty()) {
            throw new EmailProcessingException(
                    "No folders provided for processing",
                    "VALIDATION_ERROR",
                    "INPUT",
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Empty folder list")
            );
        }

        // Validate configuration
        try {
            config.validate();
        } catch (Exception e) {
            throw new EmailProcessingException(
                    "Invalid processing configuration: " + e.getMessage(),
                    "CONFIGURATION_ERROR",
                    "CONFIG",
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    e
            );
        }
    }

    private ProcessingSession initializeSession(List<FolderInfo> folderInfos) {
        int totalEmails = folderInfos.stream()
                .mapToInt(FolderInfo::messageCount)
                .sum();

        ProcessingSession session = new ProcessingSession(
                LocalDateTime.now(),
                folderInfos.size(),
                totalEmails,
                config.getThreadCount()
        );

        progressTracker.initialize(totalEmails);

        log.info("{} Processing session initialized: {}",
                LogUtils.PROCESS_EMOJI, session);

        return session;
    }

    private void startProgressReporting() {
        progressReporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress-reporter");
            t.setDaemon(true);
            return t;
        });

        progressReporter.scheduleWithFixedDelay(
                this::reportProgress,
                PROGRESS_UPDATE_INTERVAL,
                PROGRESS_UPDATE_INTERVAL,
                TimeUnit.MILLISECONDS
        );
    }

    private CompletableFuture<Void> startEmailStreamProcessing() {
        return CompletableFuture.runAsync(() -> {
            try {
                processEmailStream();
            } catch (Exception e) {
                log.error("{} Email stream processing failed: {}",
                        LogUtils.ERROR_EMOJI, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }, processingExecutor);
    }

    private void processEmailStream() throws InterruptedException {
        log.info("{} Starting email stream processing", LogUtils.PROCESS_EMOJI);

        Flux<Email> emailStream = emailQueue.asFlux()
                .doOnNext(this::processEmail)
                .doOnError(this::handleStreamError)
                .doOnComplete(() -> log.info("{} Email stream completed", LogUtils.SUCCESS_EMOJI));

        // Subscribe and wait for completion
        CountDownLatch completionLatch = new CountDownLatch(1);

        emailStream.subscribe(
                email -> {
                    // Email processed in doOnNext
                },
                error -> {
                    log.error("{} Email stream error: {}", LogUtils.ERROR_EMOJI, error.getMessage(), error);
                    completionLatch.countDown();
                },
                completionLatch::countDown
        );

        waitForStreamCompletion(completionLatch);
    }

    private void processEmail(Email email) {
        try {
            progressTracker.incrementProcessed();

            // Analyze email
            analysisService.analyzeEmail(email);

            // Update statistics
            statistics.incrementProcessed();

            if (email.isNewsletter()) {
                statistics.incrementNewsletters();
            }

            reportProgress("Processing", progressTracker.getProcessedCount(), progressTracker.getTotalCount());

        } catch (Exception e) {
            statistics.incrementErrors();
            progressTracker.incrementStreamErrors();
            log.debug("Failed to process email {}: {}", email.getId(), e.getMessage());
        }
    }

    private void handleStreamError(Throwable error) {
        statistics.incrementErrors();
        progressTracker.incrementStreamErrors();
        log.error("{} Stream processing error: {}", LogUtils.ERROR_EMOJI, error.getMessage(), error);
    }

    private void waitForStreamCompletion(CountDownLatch latch) throws InterruptedException {
        long maxWaitMs = config.getStreamTimeoutMs();

        if (!latch.await(maxWaitMs, TimeUnit.MILLISECONDS)) {
            log.warn("{} Stream processing timeout after {}ms", LogUtils.WARNING_EMOJI, maxWaitMs);
        }
    }

    private void reportProgress(String phase, long current, long total) {
        if (current % config.getProgressReportInterval() == 0) {
            double percentage = total > 0 ? (current * 100.0) / total : 0.0;

            log.info("{} {} Progress: {}/{} ({:.1f}%) - Rate: {:.1f}/s",
                    LogUtils.CHART_EMOJI, phase, current, total, percentage,
                    progressTracker.getProcessingRate());
        }
    }

    private void reportProgress() {
        if (isProcessing.get()) {
            long processed = progressTracker.getProcessedCount();
            long total = progressTracker.getTotalCount();

            log.info("{} Overall Progress: {}/{} ({:.1f}%) - Errors: {} - Rate: {:.1f}/s",
                    LogUtils.CHART_EMOJI, processed, total,
                    total > 0 ? (processed * 100.0) / total : 0.0,
                    statistics.getTotalErrors(),
                    progressTracker.getProcessingRate());
        }
    }

    private ProcessingResult createFinalResult(ProcessingSession session,
                                               BatchProcessingService.ProcessingResult batchResult) {
        long totalDuration = Duration.between(session.startTime(), LocalDateTime.now()).toMillis();

        return new ProcessingResult(
                session,
                statistics.getTotalProcessed(),
                statistics.getTotalEmitted(),
                statistics.getTotalErrors(),
                statistics.getTotalSkipped(),
                statistics.getTotalNewsletters(),
                totalDuration,
                !shutdownRequested.get(),
                progressTracker.getProcessingRate(),
                progressTracker.getStreamProcessed(),
                progressTracker.getStreamErrors()
        );
    }

    public void requestShutdown() {
        if (shutdownRequested.compareAndSet(false, true)) {
            log.info("{} Shutdown requested for Email Processing Service", LogUtils.STOP_EMOJI);
            batchProcessingService.requestShutdown();
        }
    }

    private void shutdownGracefully() {
        log.info("{} Shutting down Email Processing Service", LogUtils.STOP_EMOJI);

        if (progressReporter != null) {
            progressReporter.shutdown();
            try {
                if (!progressReporter.awaitTermination(5, TimeUnit.SECONDS)) {
                    progressReporter.shutdownNow();
                }
            } catch (InterruptedException e) {
                progressReporter.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (processingExecutor != null) {
            processingExecutor.shutdown();
            try {
                if (!processingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    processingExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                processingExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("{} Email Processing Service shutdown complete", LogUtils.SUCCESS_EMOJI);
    }

    // Getter methods
    public ProcessingStatistics getStatistics() {
        return statistics;
    }

    public boolean isProcessing() {
        return isProcessing.get();
    }

    public String getProcessingStatus() {
        if (!isProcessing.get()) {
            return "IDLE";
        }

        return String.format("PROCESSING - Processed: %d, Errors: %d, Rate: %.1f/s",
                statistics.getTotalProcessed(),
                statistics.getTotalErrors(),
                progressTracker.getProcessingRate());
    }

    // Progress tracking helper - mit korrekten Typen
    private static class ProgressTracker {
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicInteger streamProcessed = new AtomicInteger(0);
        private final AtomicInteger streamErrors = new AtomicInteger(0);
        private final AtomicLong startTime = new AtomicLong(0);
        private volatile long totalCount = 0;

        public void initialize(long total) {
            this.totalCount = total;
            this.startTime.set(System.currentTimeMillis());
            this.processedCount.set(0);
            this.streamProcessed.set(0);
            this.streamErrors.set(0);
        }

        public void incrementProcessed() {
            processedCount.incrementAndGet();
            streamProcessed.incrementAndGet();
        }

        public void incrementStreamErrors() {
            streamErrors.incrementAndGet();
        }

        public long getProcessedCount() {
            return processedCount.get();
        }

        public int getStreamProcessed() {
            return streamProcessed.get();
        }

        public int getStreamErrors() {
            return streamErrors.get();
        }

        public long getTotalCount() {
            return totalCount;
        }

        public double getProcessingRate() {
            long elapsed = System.currentTimeMillis() - startTime.get();
            return elapsed > 0 ? (processedCount.get() * 1000.0) / elapsed : 0.0;
        }
    }

    // Data classes
    public record ProcessingSession(
            LocalDateTime startTime,
            int folderCount,
            int totalEmails,
            int threadCount
    ) {
        @Override
        public String toString() {
            return String.format("Session: %d folders, %d emails, %d threads",
                    folderCount, totalEmails, threadCount);
        }
    }

    // ProcessingResult Klasse mit korrekten Typen
    public record ProcessingResult(
            ProcessingSession session,
            long processed,
            long emitted,
            long errors,
            long skipped,
            long newsletters,
            long durationMs,
            boolean completed,
            double processingRate,
            int streamProcessed,    // int statt long
            int streamErrors       // int statt long
    ) {
        // Alias-Methoden für Kompatibilität
        public long totalDurationMs() {
            return durationMs;
        }

        public double getOverallRate() {
            return processingRate;
        }

        public double getErrorRate() {
            return processed > 0 ? (errors * 100.0) / processed : 0.0;
        }

        public String getSummary() {
            return String.format(
                    "Processed: %d, Errors: %d (%.1f%%), Newsletters: %d, Rate: %.1f/s, Duration: %s",
                    processed, errors, getErrorRate(), newsletters, processingRate,
                    LogUtils.formatDurationMs(durationMs)
            );
        }
    }
}