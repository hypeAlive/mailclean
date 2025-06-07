package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.FolderBatch;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.infrastructure.EmailContentExtractor;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.infrastructure.ImapConnectionManager.ImapConnection;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import javax.mail.*;
import javax.mail.internet.MimeMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BatchWorkerService {

    private static final int CONNECTIONS_PER_THREAD = 2;
    private static final int MAX_CONNECTION_FAILURES = 5;

    private final ImapConnectionManager connectionManager;
    private final ProcessingStatistics statistics;
    private final Sinks.Many<Email> emailQueue;
    private final EmailContentExtractor contentExtractor;
    private final EmailAnalysisService analysisService;

    public BatchWorkerService(ImapConnectionManager connectionManager,
                              ProcessingStatistics statistics,
                              Sinks.Many<Email> emailQueue,
                              EmailAnalysisService analysisService) {
        this.connectionManager = connectionManager;
        this.statistics = statistics;
        this.emailQueue = emailQueue;
        this.contentExtractor = new EmailContentExtractor();
        this.analysisService = analysisService;
    }

    public void processWorkQueue(int threadId, BlockingQueue<FolderBatch> workQueue) {
        log.info("🔄 Thread {} started", threadId);

        List<ImapConnection> connectionPool = createConnectionPool(threadId);
        BlockingQueue<ImapConnection> availableConnections = new LinkedBlockingQueue<>(connectionPool);

        if (connectionPool.isEmpty()) {
            log.error("❌ Thread {} has no connections - terminating", threadId);
            return;
        }

        int batchCount = 0;
        int consecutiveFailures = 0;

        while (consecutiveFailures < MAX_CONNECTION_FAILURES) {
            FolderBatch batch = null;
            try {
                batch = workQueue.poll(1, TimeUnit.SECONDS);
                if (batch == null) {
                    if (workQueue.isEmpty()) {
                        log.info("📝 Thread {} - no more batches, terminating", threadId);
                        break;
                    }
                    continue;
                }

                batchCount++;
                ImapConnection connection = availableConnections.poll(5, TimeUnit.SECONDS);
                if (connection == null) {
                    log.warn("⏰ Thread {} Batch #{}: Timeout waiting for connection", threadId, batchCount);
                    workQueue.offer(batch);
                    consecutiveFailures++;
                    continue;
                }

                try {
                    processBatch(threadId, connection, batch, batchCount);
                    consecutiveFailures = 0;
                } finally {
                    availableConnections.offer(connection);
                }

            } catch (Exception e) {
                log.error("❌ Thread {} Batch error: {}", threadId, e.getMessage());
                consecutiveFailures++;
                if (batch != null) {
                    workQueue.offer(batch);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (consecutiveFailures >= MAX_CONNECTION_FAILURES) {
            log.error("❌ Thread {} terminated due to {} consecutive failures", threadId, consecutiveFailures);
        } else {
            log.info("🏁 Thread {} completed - {} batches processed", threadId, batchCount);
        }
    }

    private void processBatch(int threadId, ImapConnection connection, FolderBatch batch, int batchCount) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            Folder folder = null;
            try {
                if (!connection.isConnected()) {
                    log.warn("🔄 Thread {} Conn {}: Connection lost - attempting reconnection...",
                            threadId, connection.getId());

                    if (!connectionManager.reconnectConnection(connection, threadId)) {
                        throw new MessagingException("Failed to reconnect");
                    }
                }

                folder = connection.getStore().getFolder(batch.folderName());
                if (!folder.isOpen()) {
                    folder.open(Folder.READ_ONLY);
                }

                int totalMessages = folder.getMessageCount();
                if (batch.start() > totalMessages) {
                    return;
                }

                int actualEnd = Math.min(batch.end(), totalMessages);
                Message[] messages = folder.getMessages(batch.start(), actualEnd);

                fetchEnvelopes(folder, messages, threadId, connection.getId());

                processMessages(messages, batch, threadId, connection);

                return;

            } catch (Exception e) {
                log.warn("⚠️ Thread {} Conn {} Batch #{}: Error (attempt {}/3): {}",
                        threadId, connection.getId(), batchCount, attempt, e.getMessage());

                handleBatchError(folder, connection, attempt, threadId);

                if (attempt == 3) {
                    throw new RuntimeException("Batch failed after 3 attempts", e);
                }

            } finally {
                try {
                    if (folder != null && folder.isOpen()) folder.close();
                } catch (Exception e) {
                    log.debug("Folder close error: {}", e.getMessage());
                }
            }
        }
    }

    private void fetchEnvelopes(Folder folder, Message[] messages, int threadId, int connId) {
        FetchProfile quickProfile = new FetchProfile();
        quickProfile.add(FetchProfile.Item.ENVELOPE);

        long fetchStart = System.currentTimeMillis();
        try {
            folder.fetch(messages, quickProfile);
        } catch (MessagingException e) {
            log.warn("Error fetching envelopes: {}", e.getMessage());
        }
        long fetchTime = System.currentTimeMillis() - fetchStart;

        log.info("🚀 Thread {} Conn {}: Quick-Fetch {} messages in {}ms",
                threadId, connId, messages.length, fetchTime);
    }

    private void processMessages(Message[] messages, FolderBatch batch, int threadId, ImapConnection connection) {
        for (Message message : messages) {
            try {
                if (message == null) {
                    statistics.incrementProcessed();
                    continue;
                }

                MimeMessage mimeMessage = (MimeMessage) message;

                String subject = contentExtractor.extractSubjectSafely(mimeMessage);
                String from = contentExtractor.extractFromSafely(mimeMessage);
                String content = contentExtractor.extractFullContent(mimeMessage, threadId, connection.getId());

                Email email = new Email(message.getMessageNumber(), subject, from, content, batch.folderName());

                analysisService.analyzeEmail(email);

                emitEmailSafely(email);
                statistics.incrementProcessed();

            } catch (Exception e) {
                log.debug("Thread {} Conn {}: Message processing error: {}",
                        threadId, connection.getId(), e.getMessage());
                statistics.incrementProcessed();
            }
        }
    }

    private void emitEmailSafely(Email email) {
        Sinks.EmitResult result = emailQueue.tryEmitNext(email);
        if (!result.isSuccess()) {
            for (int retryCount = 0; retryCount < 3; retryCount++) {
                try {
                    Thread.sleep(10);
                    result = emailQueue.tryEmitNext(email);
                    if (result.isSuccess()) {
                        statistics.incrementEmitted();
                        return;
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } else {
            statistics.incrementEmitted();
        }
    }

    private void handleBatchError(Folder folder, ImapConnection connection, int attempt, int threadId) {
        try {
            if (folder != null && folder.isOpen()) folder.close();

            if (attempt < 3 && !connection.isConnected()) {
                log.info("🔄 Thread {} Conn {}: Attempting reconnection (attempt {}/3)",
                        threadId, connection.getId(), attempt);
                connectionManager.reconnectConnection(connection, threadId);
            }
        } catch (Exception cleanupError) {
            log.debug("Connection cleanup error: {}", cleanupError.getMessage());
        }

        try {
            Thread.sleep(1000 * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private List<ImapConnection> createConnectionPool(int threadId) {
        List<ImapConnection> connectionPool = new ArrayList<>();

        for (int i = 0; i < CONNECTIONS_PER_THREAD; i++) {
            ImapConnection conn = connectionManager.createConnection(threadId, i);
            if (conn != null) {
                connectionPool.add(conn);
                log.info("✅ Thread {} Connection {} created", threadId, i);
            } else {
                log.warn("⚠️ Thread {} Connection {} could not be created", threadId, i);
            }
        }

        return connectionPool;
    }
}