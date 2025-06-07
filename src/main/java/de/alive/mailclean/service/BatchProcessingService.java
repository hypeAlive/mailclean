package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.FolderBatch;
import de.alive.mailclean.domain.FolderInfo;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class BatchProcessingService {

    private static final int BATCH_SIZE = 500;

    private final ImapConnectionManager connectionManager;
    private final ProcessingStatistics statistics;
    private final Sinks.Many<Email> emailQueue;
    private final BatchWorkerService batchWorkerService;

    public BatchProcessingService(ImapConnectionManager connectionManager,
                                  ProcessingStatistics statistics,
                                  Sinks.Many<Email> emailQueue,
                                  EmailAnalysisService analysisService) {
        this.connectionManager = connectionManager;
        this.statistics = statistics;
        this.emailQueue = emailQueue;
        this.batchWorkerService = new BatchWorkerService(
                connectionManager, statistics, emailQueue, analysisService
        );
    }

    public void processAllFolders(List<FolderInfo> folderInfos, ExecutorService executor) {
        try {
            BlockingQueue<FolderBatch> workQueue = createWorkQueue(folderInfos);
            int threadCount = 5; // Could be configurable

            CompletableFuture<Void>[] futures = new CompletableFuture[threadCount];

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                futures[i] = CompletableFuture.runAsync(() -> {
                    batchWorkerService.processWorkQueue(threadId, workQueue);
                }, executor);
            }

            CompletableFuture.allOf(futures).get();
            log.info("🎯 All worker threads completed - {} messages processed", statistics.getTotalProcessed());

        } catch (Exception e) {
            log.error("Error in batch processing", e);
            throw new RuntimeException(e);
        }
    }

    private BlockingQueue<FolderBatch> createWorkQueue(List<FolderInfo> folderInfos) {
        BlockingQueue<FolderBatch> workQueue = new LinkedBlockingQueue<>();

        for (FolderInfo folderInfo : folderInfos) {
            for (int pos = 1; pos <= folderInfo.messageCount(); pos += BATCH_SIZE) {
                int endPos = Math.min(pos + BATCH_SIZE - 1, folderInfo.messageCount());
                workQueue.offer(new FolderBatch(folderInfo.folderName(), pos, endPos));
            }
        }

        log.info("🔥 Created: {} batches for {} folders", workQueue.size(), folderInfos.size());
        return workQueue;
    }
}