package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.FolderBatch;
import de.alive.mailclean.domain.FolderInfo;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.service.config.ProcessingConfiguration;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class BatchProcessingService {

    private static final long SHUTDOWN_TIMEOUT_MS = 30000; // 30 seconds
    private static final long PROGRESS_REPORT_INTERVAL_MS = 10000; // 10 seconds
    private static final int WORKER_THREAD_PRIORITY = Thread.NORM_PRIORITY - 1;

    private final ImapConnectionManager connectionManager;
    private final ProcessingStatistics statistics;
    private final Sinks.Many<Email> emailQueue;
    private final BatchWorkerService batchWorkerService;
    private final ProgressTrackingService progressService;
    private final ProcessingConfiguration config;

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicInteger activeFolders = new AtomicInteger(0);
    private final AtomicLong processingStartTime = new AtomicLong(0);

    private ExecutorService executorService;
    private ScheduledExecutorService progressReporter;
    private BlockingQueue<FolderBatch> workQueue;

    public BatchProcessingService(ImapConnectionManager connectionManager,
                                  ProcessingStatistics statistics,
                                  Sinks.Many<Email> emailQueue,
                                  EmailAnalysisService analysisService,
                                  ProcessingConfiguration config) {
        this.connectionManager = connectionManager;
        this.statistics = statistics;
        this.emailQueue = emailQueue;
        this.config = config;
        this.batchWorkerService = new BatchWorkerService(connectionManager, statistics, emailQueue, analysisService);
        this.progressService = new ProgressTrackingService(statistics);

        log.info("{} Batch Processing Service initialized with configuration: {}",
                LogUtils.SUCCESS_EMOJI, config.getConfigurationSummary());
    }

    public CompletableFuture<ProcessingResult> processAllFoldersAsync(List<FolderInfo> folderInfos,
                                                                      int threadCount) {
        // Verwende Konfiguration statt übergebenen Parameter
        int actualThreadCount = config.getThreadCount();
        log.info("{} Using configured thread count: {} (requested was: {})",
                LogUtils.PROCESS_EMOJI, actualThreadCount, threadCount);

        return CompletableFuture.supplyAsync(() -> {
            try {
                return processAllFolders(folderInfos, actualThreadCount);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public ProcessingResult processAllFolders(List<FolderInfo> folderInfos, int requestedThreadCount)
            throws EmailProcessingException, InterruptedException {

        // Ignoriere requestedThreadCount und verwende Konfiguration
        int threadCount = config.getThreadCount();

        validateInputs(folderInfos, threadCount);

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
            log.info("{} Starting batch processing with {} threads (configured: {})",
                    LogUtils.ROCKET_EMOJI, threadCount, config.getConfigurationSummary());

            ProcessingSession session = initializeProcessingSession(folderInfos, threadCount);
            processingStartTime.set(System.currentTimeMillis());

            startProgressReporting();

            // Verwende konfigurierte Batch-Größe
            workQueue = createWorkQueue(folderInfos);
            log.info("{} Created work queue with {} batches (batch size: {})",
                    LogUtils.CHART_EMOJI, workQueue.size(), config.getBatchSize());

            startWorkerThreads(threadCount);

            return monitorProcessing(session);

        } finally {
            shutdownGracefully();
            isProcessing.set(false);
        }
    }

    private void validateInputs(List<FolderInfo> folderInfos, int threadCount) throws EmailProcessingException {
        if (folderInfos == null || folderInfos.isEmpty()) {
            throw new EmailProcessingException(
                    "No folders provided for processing",
                    "VALIDATION_ERROR",
                    "INPUT",
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Empty folder list")
            );
        }

        if (threadCount <= 0) {
            throw new EmailProcessingException(
                    "Thread count must be positive: " + threadCount,
                    "VALIDATION_ERROR",
                    "INPUT",
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Invalid thread count")
            );
        }

        // Validiere Konfiguration
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

    private ProcessingSession initializeProcessingSession(List<FolderInfo> folderInfos, int threadCount) {
        int totalEmails = folderInfos.stream()
                .mapToInt(FolderInfo::messageCount)
                .sum();

        int estimatedBatches = calculateEstimatedBatches(folderInfos);

        return new ProcessingSession(
                UUID.randomUUID().toString().substring(0, 8),
                LocalDateTime.now(),
                folderInfos.size(),
                totalEmails,
                estimatedBatches,
                threadCount
        );
    }

    private int calculateEstimatedBatches(List<FolderInfo> folderInfos) {
        return folderInfos.stream()
                .mapToInt(folder -> {
                    int batchSize = calculateOptimalBatchSize(folder);
                    return (int) Math.ceil((double) folder.messageCount() / batchSize);
                })
                .sum();
    }

    private void startProgressReporting() {
        progressReporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress-reporter");
            t.setDaemon(true);
            return t;
        });

        progressReporter.scheduleWithFixedDelay(
                this::reportProgress,
                PROGRESS_REPORT_INTERVAL_MS,
                PROGRESS_REPORT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    private void reportProgress() {

                progressService.reportProgress();

    }

    private BlockingQueue<FolderBatch> createWorkQueue(List<FolderInfo> folderInfos) {
        // Verwende konfigurierten Buffer
        BlockingQueue<FolderBatch> queue = new LinkedBlockingQueue<>(config.getStreamBufferSize());

        int batchId = 1;
        for (FolderInfo folderInfo : folderInfos) {
            List<FolderBatch> batches = createBatchesForFolder(folderInfo, batchId);
            queue.addAll(batches);
            batchId += batches.size();
        }

        return queue;
    }

    private List<FolderBatch> createBatchesForFolder(FolderInfo folderInfo, int startingBatchId) {
        List<FolderBatch> batches = new ArrayList<>();
        int messageCount = folderInfo.messageCount();
        int batchSize = calculateOptimalBatchSize(folderInfo);

        for (int start = 1; start <= messageCount; start += batchSize) {
            int end = Math.min(start + batchSize - 1, messageCount);
            FolderBatch batch = new FolderBatch(folderInfo.folderName(), start, end);
            batches.add(batch);
        }

        log.debug("{} Created {} batches for folder '{}' ({} emails, batch size: {})",
                LogUtils.PROCESS_EMOJI, batches.size(), folderInfo.folderName(), messageCount, batchSize);

        return batches;
    }

    private int calculateOptimalBatchSize(FolderInfo folderInfo) {
        // Verwende konfigurierte Batch-Größe als Basis
        int baseBatchSize = config.getBatchSize();

        // Anpassungen basierend auf Folder-Typ
        if ("INBOX".equalsIgnoreCase(folderInfo.folderName())) {
            return Math.min(baseBatchSize, 200); // Kleinere Batches für INBOX
        }

        if (folderInfo.messageCount() > 10000) {
            return Math.min(baseBatchSize * 2, 500); // Größere Batches für große Folders
        }

        return baseBatchSize;
    }

    private void startWorkerThreads(int threadCount) {
        log.info("{} Starting {} worker threads...", LogUtils.ROCKET_EMOJI, threadCount);

        executorService = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "batch-worker-" + System.currentTimeMillis());
            t.setPriority(WORKER_THREAD_PRIORITY);
            t.setDaemon(false);
            return t;
        });

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    batchWorkerService.processWorkQueue(threadId, workQueue);
                } catch (Exception e) {
                    log.error("{} Worker thread {} failed: {}",
                            LogUtils.ERROR_EMOJI, threadId, e.getMessage(), e);
                }
            });
        }
    }

    private ProcessingResult monitorProcessing(ProcessingSession session) throws InterruptedException {
        log.info("{} Monitoring processing session: {}", LogUtils.SEARCH_EMOJI, session);

        while (!shutdownRequested.get() && !isProcessingComplete()) {
            Thread.sleep(1000);

            // Check for timeout
            long elapsed = System.currentTimeMillis() - processingStartTime.get();
            long timeoutMs = config.getProcessingTimeout().toMillis();

            if (elapsed > timeoutMs) {
                log.warn("{} Processing timeout after {}ms, requesting shutdown",
                        LogUtils.WARNING_EMOJI, elapsed);
                requestShutdown();
                break;
            }
        }

        long totalDuration = System.currentTimeMillis() - processingStartTime.get();
        double processingRate = totalDuration > 0 ? (statistics.getTotalProcessed() * 1000.0) / totalDuration : 0.0;

        return new ProcessingResult(
                session,
                statistics.getTotalProcessed(),
                statistics.getTotalEmitted(),
                statistics.getTotalErrors(),
                statistics.getTotalSkipped(),
                statistics.getTotalNewsletters(),
                totalDuration,
                !shutdownRequested.get(),
                processingRate,          // Hinzugefügt: Verarbeitungsrate
                0,                       // Hinzugefügt: streamProcessed (dummy-Wert)
                0                        // Hinzugefügt: streamErrors (dummy-Wert)
        );
    }

    // Processing result data - mit allen benötigten Feldern
    public record ProcessingResult(
            ProcessingSession session,
            long processed,
            long emitted,
            long errors,
            long skipped,
            long newsletters,
            long durationMs,
            boolean completed,
            double processingRate,      // Hinzugefügt
            int streamProcessed,        // Hinzugefügt
            int streamErrors           // Hinzugefügt
    ) {
        public double getProcessingRate() {
            return processingRate;
        }

        public double getErrorRate() {
            return processed > 0 ? (errors * 100.0) / processed : 0.0;
        }

        public String getSummary() {
            return String.format(
                    "Processed: %d, Errors: %d (%.1f%%), Rate: %.1f/s, Duration: %s",
                    processed, errors, getErrorRate(), getProcessingRate(),
                    LogUtils.formatDurationMs(durationMs)
            );
        }
    }

    private boolean isProcessingComplete() {
        return workQueue.isEmpty() &&
                batchWorkerService.getActiveBatches() == 0 &&
                activeFolders.get() == 0;
    }

    public void requestShutdown() {
        if (shutdownRequested.compareAndSet(false, true)) {
            log.info("{} Shutdown requested for Batch Processing Service", LogUtils.STOP_EMOJI);
            batchWorkerService.requestShutdown();
        }
    }

    private void shutdownGracefully() {
        log.info("{} Shutting down Batch Processing Service", LogUtils.STOP_EMOJI);

        if (progressReporter != null) {
            progressReporter.shutdown();
        }

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    log.warn("{} Forcing shutdown of worker threads", LogUtils.WARNING_EMOJI);
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        try {
            batchWorkerService.waitForCompletion(SHUTDOWN_TIMEOUT_MS / 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("{} Batch Processing Service shutdown complete", LogUtils.SUCCESS_EMOJI);
    }

    // Status and monitoring methods
    public boolean isProcessing() {
        return isProcessing.get();
    }

    public ProcessingStatistics getStatistics() {
        return statistics;
    }

    public String getProcessingStatus() {
        if (!isProcessing.get()) {
            return "IDLE";
        }

        return String.format("PROCESSING - Queue: %d, Active: %d, Threads: %d",
                workQueue != null ? workQueue.size() : 0,
                batchWorkerService.getActiveBatches(),
                config.getThreadCount());
    }

    // Processing session data
    public record ProcessingSession(
            String sessionId,
            LocalDateTime startTime,
            int folderCount,
            int totalEmails,
            int estimatedBatches,
            int threadCount
    ) {
        @Override
        public String toString() {
            return String.format("Session[%s]: %d folders, %d emails, %d threads",
                    sessionId, folderCount, totalEmails, threadCount);
        }
    }

}