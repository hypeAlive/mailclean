package de.alive.mailclean;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.ProcessingStatistics;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.service.*;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EmailCollectionOrchestrator {

    private final Configuration configuration;
    private final ImapConnectionManager connectionManager;
    private final FolderDiscoveryService folderDiscoveryService;
    private final BatchProcessingService batchProcessingService;
    private final ProcessingStatistics statistics;
    private final EmailAnalysisService analysisService;

    private final Sinks.Many<Email> emailQueue = Sinks.many().multicast().onBackpressureBuffer();
    private ExecutorService executor;

    public EmailCollectionOrchestrator(Configuration configuration, EmailAnalysisService analysisService) {
        this.configuration = configuration;
        this.analysisService = analysisService;
        this.connectionManager = new ImapConnectionManager(configuration);
        this.folderDiscoveryService = new FolderDiscoveryService(connectionManager);
        this.statistics = new ProcessingStatistics();

        this.batchProcessingService = new BatchProcessingService(
                connectionManager,
                statistics,
                emailQueue,
                analysisService
        );
    }

    public Mono<Integer> start() {
        log.info("🚀 Starting Email Collection Orchestrator");

        connectionManager.startKeepAlive();

        return Mono.fromCallable(this::executeCollection)
                .doOnSuccess(this::logCompletionStatistics);
    }

    public Flux<Email> getEmailStream() {
        return emailQueue.asFlux();
    }

    public int getTotalEmitted() {
        return statistics.getTotalEmitted();
    }

    public void shutdown() {
        log.info("🛑 Shutting down Email Collection Orchestrator");

        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("⏰ Forcing executor shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        connectionManager.shutdown();
        log.info("✅ Orchestrator shutdown completed");
    }

    private Integer executeCollection() {
        try {
            var folderInfos = folderDiscoveryService.discoverAllFolders();
            if (folderInfos.isEmpty()) {
                log.warn("❌ No folders found!");
                emailQueue.tryEmitComplete();
                return 0;
            }

            int totalMessages = folderInfos.stream().mapToInt(f -> f.messageCount()).sum();
            log.info("📁 Found: {} folders with {} total messages", folderInfos.size(), totalMessages);

            executor = Executors.newFixedThreadPool(6);
            batchProcessingService.processAllFolders(folderInfos, executor);

            emailQueue.tryEmitComplete();
            return statistics.getTotalProcessed();

        } catch (Exception e) {
            log.error("Error in email collection", e);
            emailQueue.tryEmitError(e);
            throw new RuntimeException(e);
        }
    }

    private void logCompletionStatistics(int count) {
        log.info("✅ {} emails collected and analyzed", count);
    }
}