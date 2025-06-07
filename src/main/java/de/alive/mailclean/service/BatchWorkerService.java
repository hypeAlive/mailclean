package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.FolderBatch;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.exception.EmailProcessingException;

import de.alive.mailclean.infrastructure.EmailContentExtractor;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.infrastructure.ImapConnectionManager.ImapConnection;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BatchWorkerService {

    // Reduzierte und optimierte Timeouts
    private static final int CONNECTIONS_PER_THREAD = 2; // Erhöht von 1
    private static final int MAX_CONNECTION_FAILURES = 5;
    private static final int BATCH_TIMEOUT_SECONDS = 180; // Reduziert von 300
    private static final int ENVELOPE_FETCH_TIMEOUT_MS = 15000; // Reduziert von 30000
    private static final int MESSAGE_PROCESS_TIMEOUT_MS = 30000; // Reduziert von 60000
    private static final int CONNECTION_RETRY_DELAY_MS = 2000;

    private final ImapConnectionManager connectionManager;
    private final ProcessingStatistics statistics;
    private final Sinks.Many<Email> emailQueue;
    private final EmailContentExtractor contentExtractor;
    private final EmailAnalysisService analysisService;

    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicInteger activeBatches = new AtomicInteger(0);

    public BatchWorkerService(ImapConnectionManager connectionManager,
                              ProcessingStatistics statistics,
                              Sinks.Many<Email> emailQueue,
                              EmailAnalysisService analysisService) {
        this.connectionManager = connectionManager;
        this.statistics = statistics;
        this.emailQueue = emailQueue;
        this.analysisService = analysisService;
        this.contentExtractor = new EmailContentExtractor();

        log.info("{} Batch Worker Service initialized with {} connections per thread",
                LogUtils.SUCCESS_EMOJI, CONNECTIONS_PER_THREAD);
    }

    public void processWorkQueue(int threadId, BlockingQueue<FolderBatch> workQueue) {
        statistics.threadStarted(threadId);
        List<ImapConnection> connectionPool = null;

        try {
            connectionPool = createConnectionPool(threadId);
            log.info("{} Thread {} started with {} connections",
                    LogUtils.PROCESS_EMOJI, threadId, connectionPool.size());

            int batchCount = 0;
            int consecutiveFailures = 0;

            while (!shutdownRequested.get() && !Thread.currentThread().isInterrupted()) {

                FolderBatch batch = workQueue.poll(1, TimeUnit.SECONDS);
                if (batch == null) {
                    continue; // Check for shutdown or more work
                }

                batchCount++;
                ImapConnection connection = selectBestConnection(connectionPool);

                if (connection != null) {
                    try {
                        processBatchWithTimeout(threadId, connection, batch, batchCount);
                        consecutiveFailures = 0; // Reset failure counter on success

                        // Log connection status periodically
                        if (batchCount % 10 == 0) {
                            logConnectionStatus(threadId);
                        }

                    } catch (Exception e) {
                        consecutiveFailures++;
                        log.error("{} Thread {} batch processing failed ({}): {}",
                                LogUtils.ERROR_EMOJI, threadId, consecutiveFailures, e.getMessage());

                        if (consecutiveFailures >= MAX_CONNECTION_FAILURES) {
                            log.error("{} Thread {} exceeded max failures, refreshing connections",
                                    LogUtils.ERROR_EMOJI, threadId);
                            refreshConnectionPool(connectionPool, threadId);
                            consecutiveFailures = 0;
                        }
                    }
                } else {
                    log.error("{} Thread {} has no available connections - requeueing batch {}",
                            LogUtils.ERROR_EMOJI, threadId, batch.id());
                    workQueue.offer(batch); // Requeue for retry
                    Thread.sleep(CONNECTION_RETRY_DELAY_MS);

                    // Versuche Connection Pool zu refreshen
                    refreshConnectionPool(connectionPool, threadId);
                }
            }

        } catch (InterruptedException e) {
            log.info("{} Thread {} interrupted - shutting down gracefully",
                    LogUtils.STOP_EMOJI, threadId);
            Thread.currentThread().interrupt();

        } catch (Exception e) {
            log.error("{} Thread {} crashed with unexpected error: {}",
                    LogUtils.ERROR_EMOJI, threadId, e.getMessage(), e);

        } finally {
            cleanupConnectionPool(connectionPool, threadId);
            statistics.threadCompleted(threadId);

            log.info("{} Thread {} completed", LogUtils.SUCCESS_EMOJI, threadId);
        }
    }

    private void logConnectionStatus(int threadId) {
        if (threadId == 0) { // Nur Thread 0 loggt Pool-Status um Spam zu vermeiden
            log.info("{} Pool Status: {} available, {} active, {} total",
                    LogUtils.CHART_EMOJI,
                    connectionManager.getAvailableConnections(),
                    connectionManager.getActiveConnections(),
                    connectionManager.getTotalConnectionsCreated());
        }
    }

    private void refreshConnectionPool(List<ImapConnection> connectionPool, int threadId) {
        log.info("{} Thread {} refreshing connection pool", LogUtils.PROCESS_EMOJI, threadId);

        // Entferne und schließe alle bestehenden Verbindungen
        if (connectionPool != null) {
            for (ImapConnection conn : connectionPool) {
                if (conn != null) {
                    connectionManager.invalidateConnection(conn);
                }
            }
            connectionPool.clear();
        }

        // Erstelle neue Verbindungen
        try {
            for (int i = 0; i < CONNECTIONS_PER_THREAD; i++) {
                try {
                    ImapConnection newConnection = connectionManager.acquireConnection();
                    if (newConnection != null) {
                        connectionPool.add(newConnection);
                    }
                } catch (Exception e) {
                    log.warn("Failed to create replacement connection {}: {}", i, e.getMessage());
                }
            }
            log.info("{} Thread {} refreshed pool with {} connections",
                    LogUtils.SUCCESS_EMOJI, threadId, connectionPool.size());
        } catch (Exception e) {
            log.error("{} Thread {} failed to refresh connection pool: {}",
                    LogUtils.ERROR_EMOJI, threadId, e.getMessage());
        }
    }

    private void processBatchWithTimeout(int threadId, ImapConnection connection,
                                         FolderBatch batch, int batchCount) {

        CompletableFuture<Void> batchTask = CompletableFuture.runAsync(() -> {
            try {
                processBatch(threadId, connection, batch, batchCount);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            batchTask.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        } catch (Exception e) {
            batchTask.cancel(true);

            log.error("{} Thread {} batch {} timed out after {} seconds",
                    LogUtils.SEARCH_EMOJI, threadId, batch.id(), BATCH_TIMEOUT_SECONDS);

            statistics.recordThreadActivity(threadId, "TIMEOUT", 1);
            statistics.incrementErrors();

            // Force reconnection after timeout
            connectionManager.invalidateConnection(connection);
        }
    }

    private void processBatch(int threadId, ImapConnection connection,
                              FolderBatch batch, int batchCount) throws Exception {

        activeBatches.incrementAndGet();
        statistics.batchStarted();

        long batchStartTime = System.nanoTime(); // Verwende nanoTime für präzise Messung
        int processed = 0;

        try {
            log.info("{} Thread {} processing batch {} - {} ({}-{}) = {} emails",
                    LogUtils.PROCESS_EMOJI, threadId, batchCount,
                    batch.folderName(), batch.start(), batch.end(), batch.size());

            Folder folder = openFolder(connection, batch.folderName());

            try {
                Message[] messages = fetchMessagesWithTimeout(folder, batch);
                processed = processMessagesWithTimeout(messages, batch, threadId, connection);

                statistics.recordThreadActivity(threadId, "PROCESSED", processed);
                statistics.recordFolderActivity(batch.folderName(), processed, 0);

            } finally {
                closeFolderSafely(folder, threadId);
            }

            long durationNanos = System.nanoTime() - batchStartTime;
            long durationMs = durationNanos / 1_000_000;
            double rate = calculateProcessingRate(processed, durationMs);

            log.info("{} Thread {} completed batch {} in {} - {} emails at {:.1f}/s",
                    LogUtils.SUCCESS_EMOJI, threadId, batchCount,
                    LogUtils.formatDurationMs(durationMs), processed, rate);

        } catch (Exception e) {
            long durationNanos = System.nanoTime() - batchStartTime;
            long durationMs = durationNanos / 1_000_000;

            log.error("{} Thread {} batch {} failed after {} with {} processed: {}",
                    LogUtils.ERROR_EMOJI, threadId, batchCount,
                    LogUtils.formatDurationMs(durationMs), processed, e.getMessage());

            statistics.recordThreadActivity(threadId, "ERRORS", 1);
            statistics.recordFolderActivity(batch.folderName(), processed, 1);
            statistics.incrementErrors();

            throw e;

        } finally {
            activeBatches.decrementAndGet();
            statistics.batchCompleted();
        }
    }

    private double calculateProcessingRate(int processed, long durationMs) {
        return durationMs > 0 ? (processed * 1000.0) / durationMs : 0.0;
    }

    private Folder openFolder(ImapConnection connection, String folderName) throws Exception {
        try {
            Folder folder = connection.getStore().getFolder(folderName);
            if (!folder.exists()) {
                throw new EmailProcessingException(
                        "Folder does not exist: " + folderName,
                        "BATCH_PROCESSING",
                        folderName,
                        EmailProcessingException.ProcessingStage.FOLDER_ACCESS,
                        new IllegalStateException("Folder not found")
                );
            }

            folder.open(Folder.READ_ONLY);
            return folder;

        } catch (MessagingException e) {
            throw new EmailProcessingException(
                    "Failed to open folder: " + folderName,
                    "BATCH_PROCESSING",
                    folderName,
                    EmailProcessingException.ProcessingStage.FOLDER_ACCESS,
                    e
            );
        }
    }

    private Message[] fetchMessagesWithTimeout(Folder folder, FolderBatch batch) throws Exception {
        CompletableFuture<Message[]> fetchTask = CompletableFuture.supplyAsync(() -> {
            try {
                return folder.getMessages(batch.start(), batch.end());
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            Message[] messages = fetchTask.get(ENVELOPE_FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            log.debug("{} Fetched {} messages from {} ({}-{})",
                    LogUtils.EMAIL_EMOJI, messages.length,
                    batch.folderName(), batch.start(), batch.end());

            return messages;

        } catch (Exception e) {
            fetchTask.cancel(true);

            throw new EmailProcessingException(
                    String.format("Message fetch timeout for %s (%d-%d) after %dms",
                            batch.folderName(), batch.start(), batch.end(), ENVELOPE_FETCH_TIMEOUT_MS),
                    "BATCH_PROCESSING",
                    batch.folderName(),
                    EmailProcessingException.ProcessingStage.MESSAGE_FETCH,
                    e
            );
        }
    }

    private int processMessagesWithTimeout(Message[] messages, FolderBatch batch,
                                           int threadId, ImapConnection connection) throws Exception {

        CompletableFuture<Integer> processTask = CompletableFuture.supplyAsync(() -> {
            try {
                return processMessages(messages, batch, threadId, connection);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            return processTask.get(MESSAGE_PROCESS_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            processTask.cancel(true);

            throw new EmailProcessingException(
                    String.format("Message processing timeout for %s after %dms",
                            batch.folderName(), MESSAGE_PROCESS_TIMEOUT_MS),
                    "BATCH_PROCESSING",
                    batch.folderName(),
                    EmailProcessingException.ProcessingStage.MESSAGE_PROCESSING,
                    e
            );
        }
    }

    private int processMessages(Message[] messages, FolderBatch batch,
                                int threadId, ImapConnection connection) throws Exception {

        int processed = 0;
        List<Email> emailBatch = new ArrayList<>();

        for (int i = 0; i < messages.length; i++) {
            try {
                Message message = messages[i];
                if (message == null) {
                    log.debug("Skipping null message at index {}", i);
                    continue;
                }

                Email email = contentExtractor.extractEmail(message, batch.folderName(), threadId, connection.getId());
                if (email != null) {
                    // Add batch metadata
                    email.addMetadata("batch_id", batch.id());
                    email.addMetadata("thread_id", String.valueOf(threadId));
                    email.addMetadata("connection_id", String.valueOf(connection.getId()));
                    email.addMetadata("processed_at", LocalDateTime.now().toString());

                    emailBatch.add(email);
                    processed++;

                    // Process in smaller sub-batches for better memory management
                    if (emailBatch.size() >= 25) { // Reduziert von 50 für bessere Reaktionszeit
                        processBatchOfEmails(emailBatch, threadId);
                        emailBatch.clear();
                    }
                }

                if (processed % 50 == 0) { // Reduziert von 100
                    log.debug("{} Thread {} progress: {}/{} emails from {}",
                            LogUtils.PROCESS_EMOJI, threadId, processed,
                            messages.length, batch.folderName());
                }

            } catch (Exception e) {
                log.warn("{} Thread {} failed to process message {}: {}",
                        LogUtils.WARNING_EMOJI, threadId, (batch.start() + i), e.getMessage());

                statistics.incrementErrors();
                // Continue with next message
            }
        }

        // Process remaining emails
        if (!emailBatch.isEmpty()) {
            processBatchOfEmails(emailBatch, threadId);
        }

        return processed;
    }

    private void processBatchOfEmails(List<Email> emails, int threadId) {
        try {
            // Analyze emails in batch if the service supports it
            EmailAnalysisService.BatchAnalysisResult result = analysisService.analyzeBatch(emails);

            if (result.getFailed() > 0) {
                log.debug("{} Thread {} analysis batch: {} successful, {} failed",
                        LogUtils.WARNING_EMOJI, threadId, result.getSuccessful(), result.getFailed());
            }

            // Emit all emails to the processing queue
            for (Email email : emails) {
                emitEmailSafely(email);
                statistics.incrementProcessed();
            }

        } catch (Exception e) {
            log.error("{} Thread {} failed to process email batch: {}",
                    LogUtils.ERROR_EMOJI, threadId, e.getMessage());

            // Try to emit emails individually
            for (Email email : emails) {
                try {
                    analysisService.analyzeEmail(email);
                    emitEmailSafely(email);
                    statistics.incrementProcessed();
                } catch (Exception emailError) {
                    log.debug("Failed to process individual email: {}", emailError.getMessage());
                    statistics.incrementErrors();
                }
            }
        }
    }

    private void emitEmailSafely(Email email) {
        try {
            Sinks.EmitResult result = emailQueue.tryEmitNext(email);

            if (result.isSuccess()) {
                statistics.incrementEmitted();
            } else {
                log.warn("{} Failed to emit email to queue: {} - {}",
                        LogUtils.WARNING_EMOJI, result, email.getId());
                statistics.incrementErrors();
            }

        } catch (Exception e) {
            log.error("{} Error emitting email to queue: {}",
                    LogUtils.ERROR_EMOJI, e.getMessage());
            statistics.incrementErrors();
        }
    }

    private void closeFolderSafely(Folder folder, int threadId) {
        try {
            if (folder != null && folder.isOpen()) {
                folder.close();
                log.debug("{} Thread {} closed folder: {}",
                        LogUtils.SUCCESS_EMOJI, threadId, folder.getFullName());
            }
        } catch (Exception e) {
            log.debug("Error closing folder in thread {}: {}", threadId, e.getMessage());
        }
    }

    private List<ImapConnection> createConnectionPool(int threadId) {
        List<ImapConnection> connections = new ArrayList<>();

        for (int i = 0; i < CONNECTIONS_PER_THREAD; i++) {
            try {
                ImapConnection connection = connectionManager.acquireConnection();
                connections.add(connection);

                log.debug("{} Thread {} acquired connection {}/{}",
                        LogUtils.SUCCESS_EMOJI, threadId, i + 1, CONNECTIONS_PER_THREAD);

            } catch (Exception e) {
                log.error("{} Thread {} failed to acquire connection {}: {}",
                        LogUtils.ERROR_EMOJI, threadId, i + 1, e.getMessage());
            }
        }

        if (connections.isEmpty()) {
            throw new RuntimeException("Failed to acquire any connections for thread " + threadId);
        }

        return connections;
    }

    private ImapConnection selectBestConnection(List<ImapConnection> connections) {
        // Erste Wahl: Gesunde Verbindungen
        Optional<ImapConnection> healthy = connections.stream()
                .filter(conn -> conn != null && isConnectionHealthy(conn))
                .findFirst();

        if (healthy.isPresent()) {
            return healthy.get();
        }

        // Fallback: Versuche Reconnection
        for (ImapConnection conn : connections) {
            if (conn != null && attemptReconnection(conn)) {
                return conn;
            }
        }

        return null;
    }

    private boolean isConnectionHealthy(ImapConnection connection) {
        try {
            return connection.isConnected() &&
                    connection.getStore().isConnected();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean attemptReconnection(ImapConnection connection) {
        try {
            if (!connection.isConnected()) {
                // Connection Manager wird automatisch reconnecten
                connection.performKeepAlive();
            }
            return connection.isConnected();
        } catch (Exception e) {
            log.debug("Reconnection attempt failed for connection {}: {}",
                    connection.getId(), e.getMessage());
            return false;
        }
    }

    private void cleanupConnectionPool(List<ImapConnection> connections, int threadId) {
        if (connections != null) {
            for (ImapConnection connection : connections) {
                try {
                    if (connection != null) {
                        connectionManager.releaseConnection(connection);
                    }
                } catch (Exception e) {
                    log.debug("Error releasing connection in thread {}: {}", threadId, e.getMessage());
                }
            }

            log.debug("{} Thread {} released {} connections",
                    LogUtils.SUCCESS_EMOJI, threadId, connections.size());
        }
    }

    public void requestShutdown() {
        log.info("{} Shutdown requested for Batch Worker Service", LogUtils.STOP_EMOJI);
        shutdownRequested.set(true);
    }

    public boolean isShutdownRequested() {
        return shutdownRequested.get();
    }

    public int getActiveBatches() {
        return activeBatches.get();
    }

    public void waitForCompletion(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (activeBatches.get() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        if (activeBatches.get() > 0) {
            log.warn("{} Timeout waiting for {} active batches to complete",
                    LogUtils.WARNING_EMOJI, activeBatches.get());
        } else {
            log.info("{} All batches completed successfully", LogUtils.SUCCESS_EMOJI);
        }
    }
}