package de.alive.mailclean.infrastructure;

import de.alive.mailclean.service.Batch;
import de.alive.mailclean.service.BatchEntity;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

@Slf4j
public class BatchProcessor {

    private final int threadCount;
    private final Scheduler workerScheduler;
    private final ConnectionManager connectionManager;
    private final ConcurrentLinkedQueue<IConnection> connectionPool = new ConcurrentLinkedQueue<>();
    private final Semaphore connectionSemaphore;
    private final AtomicBoolean connectionsInitialized = new AtomicBoolean(false);

    public BatchProcessor(@NotNull ConnectionManager manager, int threadCount) {
        // ✅ Limitiere auf max 10 Connections für Gmail
        this.threadCount = Math.min(threadCount, 10);
        this.connectionManager = manager;
        this.workerScheduler = Schedulers.newParallel("batch-worker", this.threadCount, true);
        this.connectionSemaphore = new Semaphore(this.threadCount);

        log.info("{} BatchProcessor initialized with {} threads (limited for Gmail stability)",
                LogUtils.PROCESS_EMOJI, this.threadCount);
    }

    public Flux<BatchEntity> processAllBatches(List<Batch> batches, BiFunction<BatchEntity, IConnection, BatchEntity> processor) {
        log.info("{} Starting batch processing: {} batches", LogUtils.ROCKET_EMOJI, batches.size());

        return initializeConnections()
                .then(Mono.just(batches))
                .flatMapMany(batchList ->
                        Flux.fromIterable(batchList)
                                // ✅ Keine parallele Aufteilung, sondern sequenzielle Verarbeitung mit begrenzter Concurrency
                                .flatMap(batch -> processBatch(batch, processor), threadCount)
                                // ✅ Verwende den workerScheduler für die Ausführung
                                .subscribeOn(workerScheduler)
                );
    }

    private Mono<Void> initializeConnections() {
        if (connectionsInitialized.get()) {
            return Mono.empty();
        }

        return Mono.fromRunnable(() -> {
                    log.info("{} Initializing {} connections...", LogUtils.PROCESS_EMOJI, threadCount);
                })
                .then(Flux.range(0, threadCount)
                        .flatMap(i -> connectionManager.createConnection()
                                .doOnNext(connection -> {
                                    connectionPool.offer(connection);
                                    log.debug("{} Connection {}/{} created and added to pool",
                                            LogUtils.SUCCESS_EMOJI, i + 1, threadCount);
                                })
                                .doOnError(error ->
                                        log.error("{} Failed to create connection {}: {}",
                                                LogUtils.ERROR_EMOJI, i + 1, error.getMessage()))
                        )
                        .then()
                )
                .doOnSuccess(v -> {
                    connectionsInitialized.set(true);
                    log.info("{} All {} connections initialized successfully",
                            LogUtils.SUCCESS_EMOJI, threadCount);
                })
                .doOnError(error ->
                        log.error("{} Connection initialization failed: {}",
                                LogUtils.ERROR_EMOJI, error.getMessage()));
    }

    private Flux<BatchEntity> processBatch(Batch batch, BiFunction<BatchEntity, IConnection, BatchEntity> processor) {
        return acquireConnection()
                .flatMapMany(connection -> {
                    return batch.entities(connection)
                            .map(entity -> processor.apply(entity, connection))
                            .doFinally(signalType -> {
                                // ✅ Sofortiger Release im selben Thread
                                releaseConnection(connection);
                                log.debug("{} Connection {} released after batch completion",
                                        LogUtils.SUCCESS_EMOJI, connection.getId());
                            });
                })
                .onErrorResume(error -> {
                    log.error("{} Batch processing failed, skipping batch: {}",
                            LogUtils.ERROR_EMOJI, error.getMessage());
                    return Flux.empty();
                });
    }

    private Mono<IConnection> acquireConnection() {
        return Mono.fromCallable(() -> {
                    try {
                        // ✅ Längerer Timeout da wir jetzt warten können
                        boolean acquired = connectionSemaphore.tryAcquire(60, TimeUnit.SECONDS);
                        if (!acquired) {
                            throw new RuntimeException("Timeout waiting for connection semaphore");
                        }
                        return connectionPool.poll();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for connection", e);
                    }
                })
                .flatMap(connection -> {
                    if (connection != null && connection.isConnected()) {
                        return Mono.just(connection);
                    } else if (connection != null) {
                        // ✅ Simple reconnect mit graceful fallback
                        log.warn("{} Connection {} broken, attempting reconnect...",
                                LogUtils.WARNING_EMOJI, connection.getId());

                        return connectionManager.reconnectConnection(connection)
                                .flatMap(reconnected -> {
                                    if (Boolean.TRUE.equals(reconnected)) {
                                        return Mono.just(connection);
                                    } else {
                                        log.error("{} Reconnect failed for connection {}, skipping batch",
                                                LogUtils.ERROR_EMOJI, connection.getId());
                                        connectionSemaphore.release();
                                        return Mono.empty(); // ✅ Skip this batch gracefully
                                    }
                                })
                                .onErrorResume(error -> {
                                    log.error("{} Reconnect error: {}", LogUtils.ERROR_EMOJI, error.getMessage());
                                    connectionSemaphore.release();
                                    return Mono.empty(); // ✅ Skip this batch gracefully
                                });
                    } else {
                        connectionSemaphore.release();
                        return Mono.error(new RuntimeException("No connection available from pool"));
                    }
                })
                .timeout(Duration.ofSeconds(70)); // ✅ Längerer Timeout passend zum Semaphore-Timeout
    }

    private void releaseConnection(IConnection connection) {
        if (connection != null) {
            connectionPool.offer(connection);
            log.debug("{} Connection {} returned to pool", LogUtils.SUCCESS_EMOJI, connection.getId());
        }
        connectionSemaphore.release();
    }

    public void shutdown() {
        log.info("{} Shutting down BatchProcessor...", LogUtils.STOP_EMOJI);

        IConnection connection;
        while ((connection = connectionPool.poll()) != null) {
            IConnection finalConnection = connection;
            connectionManager.closeConnection(finalConnection)
                    .block(Duration.ofSeconds(5));
        }

        workerScheduler.dispose();
        log.info("{} BatchProcessor shutdown complete", LogUtils.SUCCESS_EMOJI);
    }

    public int getActualThreadCount() {
        return threadCount;
    }
}