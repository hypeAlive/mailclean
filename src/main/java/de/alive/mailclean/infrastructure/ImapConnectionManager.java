package de.alive.mailclean.infrastructure;

import de.alive.mailclean.Configuration;
import de.alive.mailclean.util.LogUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ImapConnectionManager implements ConnectionManager {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(3);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(2);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(15);
    private static final int MAX_RETRIES = 3;

    @NotNull
    private final Configuration configuration;
    @NotNull
    private final Properties imapProperties;
    @NotNull
    private final Map<Store, Connection> activeConnections = new ConcurrentHashMap<>();
    @NotNull
    private final AtomicInteger connectionIdGenerator = new AtomicInteger(0);

    public ImapConnectionManager(@NotNull Configuration configuration) {
        this.configuration = configuration;
        this.imapProperties = createImapProperties();
    }

    @NotNull
    @Override
    public Mono<IConnection> createConnection() {
        return connectWithRetry()
                .map(this::registerConnection)
                .doOnSuccess(conn -> log.info("{} IMAP connection {} created",
                        LogUtils.SUCCESS_EMOJI, conn.getId()))
                .doOnError(error -> log.error("{} Failed to create IMAP connection: {}",
                        LogUtils.ERROR_EMOJI, error.getMessage()));
    }

    @NotNull
    @Override
    public Mono<Boolean> closeConnection(@NotNull IConnection connection) {
        Connection imapConnection = (Connection) connection;
        return Mono.fromCallable(() -> activeConnections.get(imapConnection.getStore()))
                .flatMap(foundConnection -> foundConnection == null ?
                        handleConnectionNotFound(imapConnection) :
                        performCloseConnection(foundConnection))
                .onErrorResume(error -> handleCloseError(imapConnection, error));
    }

    @NotNull
    @Override
    public Mono<Boolean> reconnectConnection(@NotNull IConnection connection) {
        Connection imapConnection = (Connection) connection;
        return Mono.just(imapConnection.getStore())
                .flatMap(oldStore -> closeStore(oldStore)
                        .then(Mono.delay(RECONNECT_DELAY))
                        .then(connectWithRetry())
                        .doOnSuccess(newStore -> updateConnection(imapConnection, oldStore, newStore))
                        .thenReturn(true))
                .onErrorResume(error -> handleReconnectError(imapConnection, error));
    }

    @NotNull
    private IConnection registerConnection(@NotNull Store store) {
        int connectionId = connectionIdGenerator.incrementAndGet();
        Connection connection = new Connection(store, connectionId);
        activeConnections.put(store, connection);
        return connection;
    }

    @NotNull
    private Mono<Boolean> handleConnectionNotFound(@NotNull Connection connection) {
        log.warn("{} Connection {} not found or already closed",
                LogUtils.WARNING_EMOJI, connection.getId());
        return Mono.just(false);
    }

    @NotNull
    private Mono<Boolean> performCloseConnection(@NotNull Connection connection) {
        return closeStore(connection.getStore())
                .doOnSuccess(v -> {
                    activeConnections.remove(connection.getStore());
                    log.info("{} Connection {} closed successfully",
                            LogUtils.SUCCESS_EMOJI, connection.getId());
                })
                .thenReturn(true);
    }

    @NotNull
    private Mono<Boolean> handleCloseError(@NotNull Connection connection, @NotNull Throwable error) {
        log.error("{} Failed to close connection {}: {}",
                LogUtils.ERROR_EMOJI, connection.getId(), error.getMessage());
        return Mono.just(false);
    }

    @NotNull
    private Mono<Boolean> handleReconnectError(@NotNull Connection connection, @NotNull Throwable error) {
        log.error("{} Reconnection failed for connection {}: {}",
                LogUtils.ERROR_EMOJI, connection.getId(), error.getMessage());
        return Mono.just(false);
    }

    private void updateConnection(@NotNull Connection connection, @NotNull Store oldStore, @NotNull Store newStore) {
        connection.setStore(newStore);
        activeConnections.remove(oldStore);
        activeConnections.put(newStore, connection);
        log.info("{} Connection {} reconnected successfully",
                LogUtils.SUCCESS_EMOJI, connection.getId());
    }

    @NotNull
    private Mono<Void> closeStore(@Nullable Store store) {
        return Mono.fromCallable(() -> {
                    if (store != null && store.isConnected()) {
                        store.close();
                        log.debug("{} Store closed successfully", LogUtils.SUCCESS_EMOJI);
                    }
                    return null;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(MessagingException.class, e -> {
                    log.debug("{} Error closing store: {}", LogUtils.WARNING_EMOJI, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    @NotNull
    private Mono<Store> connectWithRetry() {
        return Mono.fromCallable(() -> {
                    Session session = Session.getInstance(imapProperties);
                    Store store = session.getStore("imaps");
                    store.connect(configuration.username(), configuration.password());
                    return store;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .retryWhen(Retry.backoff(MAX_RETRIES, RETRY_DELAY)
                        .maxBackoff(MAX_BACKOFF)
                        .jitter(0.2)
                        .filter(MessagingException.class::isInstance)
                        .doBeforeRetry(retrySignal -> log.warn("IMAP connection attempt {} failed: {}",
                                retrySignal.totalRetries() + 1, retrySignal.failure().getMessage())));
    }

    @NotNull
    private Properties createImapProperties() {
        Properties props = new Properties();

        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.host", "imap.gmail.com");
        props.setProperty("mail.imaps.port", "993");
        props.setProperty("mail.imaps.ssl.enable", "true");
        props.setProperty("mail.imaps.ssl.trust", "*");

        props.setProperty("mail.imaps.connectionpoolsize", "2");
        props.setProperty("mail.imaps.connectionpooltimeout", "5000");
        props.setProperty("mail.imaps.connectiontimeout", "60000");
        props.setProperty("mail.imaps.timeout", "60000");
        props.setProperty("mail.imaps.writetimeout", "60000");

        props.setProperty("mail.imaps.usesocketchannels", "false");
        props.setProperty("mail.imaps.enablestarttls", "true");
        props.setProperty("mail.imaps.socketFactory.fallback", "false");
        props.setProperty("mail.imaps.peek", "true");
        props.setProperty("mail.imaps.fetchsize", "8192");
        props.setProperty("mail.imaps.partialfetch", "false");
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
            return store.isConnected();
        }
    }
}