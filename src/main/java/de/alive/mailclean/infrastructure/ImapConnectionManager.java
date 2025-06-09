package de.alive.mailclean.infrastructure;

import de.alive.mailclean.Configuration;
import de.alive.mailclean.util.LogUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.mail.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ImapConnectionManager implements ConnectionManager {

    private final Configuration configuration;
    private final Properties imapProperties;
    private final List<Store> activeStores = new ArrayList<>();
    private final Map<Store, Connection> activeConnections = new HashMap<>();
    private final Object storesLock = new Object();
    private final AtomicInteger connectionIdGenerator = new AtomicInteger(0);

    public ImapConnectionManager(Configuration configuration) {
        this.configuration = configuration;
        this.imapProperties = createImapProperties();
    }

    @NotNull
    @Override
    public Mono<IConnection> createConnection() {
        return connectWithRetry()
                .map(store -> {
                    int connectionId = connectionIdGenerator.incrementAndGet();
                    Connection connection = new Connection(store, connectionId);
                    synchronized (storesLock) {
                        activeStores.add(store);
                        activeConnections.put(store, connection);
                    }
                    return (IConnection) connection;
                })
                .doOnSuccess(connection -> {
                    Connection conn = (Connection) connection;
                    log.info("{} IMAP connection {} created successfully",
                            LogUtils.SUCCESS_EMOJI, conn.getId());
                })
                .doOnError(error ->
                        log.error("{} Failed to create IMAP connection: {}",
                                LogUtils.ERROR_EMOJI, error.getMessage())
                );
    }

    @NotNull
    public Mono<Boolean> closeConnection(@NotNull IConnection connection) {
        Connection imapConnection = (Connection) connection;
        Store store = imapConnection.getStore();

        return Mono.fromCallable(() -> {
                    synchronized (storesLock) {
                        return activeConnections.get(store);
                    }
                })
                .flatMap(foundConnection -> {
                    if (foundConnection == null) {
                        log.warn("{} Connection {} not found or already closed",
                                LogUtils.WARNING_EMOJI, imapConnection.getId());
                        return Mono.just(false);
                    }

                    return closeStore(foundConnection.getStore())
                            .doOnSuccess(v -> {
                                synchronized (storesLock) {
                                    activeConnections.remove(store);
                                    activeStores.remove(foundConnection.getStore());
                                }
                                log.info("{} Connection {} closed successfully",
                                        LogUtils.SUCCESS_EMOJI, imapConnection.getId());
                            })
                            .thenReturn(true);
                })
                .onErrorResume(error -> {
                    log.error("{} Failed to close connection {}: {}",
                            LogUtils.ERROR_EMOJI, imapConnection.getId(), error.getMessage());
                    return Mono.just(false);
                });
    }

    @NotNull
    @Override
    public Mono<Boolean> reconnectConnection(@NotNull IConnection connection) {
        ImapConnectionManager.Connection imapConnection = (ImapConnectionManager.Connection) connection;
        return Mono.fromCallable(imapConnection::getStore)
                .flatMap(oldStore -> closeStore(oldStore)
                        .then(Mono.delay(Duration.ofSeconds(3))) // ✅ Longer delay for Gmail cleanup
                        .then(connectWithRetry())
                        .doOnSuccess(newStore -> {
                            imapConnection.setStore(newStore);
                            updateActiveStores(oldStore, newStore);
                            log.info("{} Connection {} reconnected successfully",
                                    LogUtils.SUCCESS_EMOJI, imapConnection.getId());
                        })
                        .thenReturn(true)
                )
                .onErrorResume(error -> {
                    log.error("{} Reconnection failed for connection {}: {}",
                            LogUtils.ERROR_EMOJI, imapConnection.getId(), error.getMessage());
                    return Mono.just(false);
                });
    }

    private Mono<Void> closeStore(Store oldStore) {
        return Mono.fromRunnable(() -> {
                    if (oldStore != null && oldStore.isConnected()) {
                        try {
                            // ✅ Close all open folders first
                            if (oldStore instanceof com.sun.mail.imap.IMAPStore) {
                                // Force close all folders
                                oldStore.close();
                            } else {
                                oldStore.close();
                            }
                            log.debug("{} Old store closed successfully", LogUtils.SUCCESS_EMOJI);
                        } catch (MessagingException e) {
                            log.debug("{} Error closing old store: {}", LogUtils.WARNING_EMOJI, e.getMessage());
                        }
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    private void updateActiveStores(Store oldStore, Store newStore) {
        synchronized (storesLock) {
            Connection connection = activeConnections.remove(oldStore);
            if (connection != null) {
                activeConnections.put(newStore, connection);
            }

            activeStores.remove(oldStore);
            activeStores.add(newStore);
        }
    }

    private Mono<Store> connectWithRetry() {
        return Mono.fromCallable(() -> {
                    Session session = Session.getInstance(imapProperties);
                    Store store = session.getStore("imaps");
                    store.connect(configuration.username(), configuration.password());
                    log.debug("✅ IMAP connection established");
                    return store;
                })
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)) // ✅ Longer initial delay
                        .maxBackoff(Duration.ofSeconds(15))
                        .jitter(0.2)
                        .filter(throwable -> throwable instanceof MessagingException)
                        .doBeforeRetry(retrySignal ->
                                log.warn("⚠️ IMAP connection attempt {} failed: {}",
                                        retrySignal.totalRetries() + 1,
                                        retrySignal.failure().getMessage()))
                )
                .doOnSuccess(store -> log.debug("🎯 IMAP connection successful"))
                .doOnError(error -> log.error("❌ IMAP connection failed after retries: {}", error.getMessage()));
    }

    // ✅ Optimierte IMAP Properties für Gmail Stabilität
    private Properties createImapProperties() {
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.host", "imap.gmail.com");
        props.setProperty("mail.imaps.port", "993");
        props.setProperty("mail.imaps.ssl.enable", "true");
        props.setProperty("mail.imaps.ssl.trust", "*");

        // ✅ DISABLE Connection Pooling - verursacht Probleme mit Gmail
        props.setProperty("mail.imaps.connectionpoolsize", "1");
        props.setProperty("mail.imaps.connectionpooltimeout", "5000");

        // ✅ LONGER Timeouts für Gmail Stabilität
        props.setProperty("mail.imaps.connectiontimeout", "60000");  // 60s statt 30s
        props.setProperty("mail.imaps.timeout", "60000");           // 60s statt 30s
        props.setProperty("mail.imaps.writetimeout", "60000");      // Write timeout

        // ✅ Keep-Alive & Performance Settings
        props.setProperty("mail.imaps.usesocketchannels", "false");
        props.setProperty("mail.imaps.enablestarttls", "true");
        props.setProperty("mail.imaps.socketFactory.fallback", "false");

        // ✅ Gmail spezifische Optimierungen
        props.setProperty("mail.imaps.peek", "true");              // Weniger Server Load
        props.setProperty("mail.imaps.fetchsize", "8192");         // Kleinere Fetch Size
        props.setProperty("mail.imaps.partialfetch", "false");     // Komplette Messages

        // ✅ Additional stability settings
        props.setProperty("mail.imaps.finalizecleanclose", "false");
        props.setProperty("mail.imaps.closefoldersonidle", "false");

        return props;
    }

    @Getter
    @AllArgsConstructor
    public static class Connection implements IConnection {

        @Setter
        @NotNull
        private Store store;
        private final int id;

        public boolean isConnected() {
            try {
                return store != null && store.isConnected();
            } catch (Exception e) {
                log.debug("Connection check failed for {}: {}", id, e.getMessage());
                return false;
            }
        }
    }
}