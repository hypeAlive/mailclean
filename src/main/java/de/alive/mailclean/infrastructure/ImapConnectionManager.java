package de.alive.mailclean.infrastructure;

import de.alive.mailclean.Configuration;
import de.alive.mailclean.exception.MailConnectionException;
import de.alive.mailclean.service.ConfigurationService;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ImapConnectionManager {

    private final Configuration configuration;
    private final Properties imapProperties;
    private final BlockingQueue<ImapConnection> connectionPool;
    private final ScheduledExecutorService keepAliveExecutor;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicInteger totalConnectionsCreated = new AtomicInteger(0);

    // Erhöhte Pool-Größe für bessere Performance
    private static final int MAX_POOL_SIZE = 16;
    private static final int CORE_POOL_SIZE = 8;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long KEEP_ALIVE_INTERVAL_SECONDS = 30; // Reduziert von 45

    public ImapConnectionManager(Configuration configuration, ConfigurationService configService) {
        this.configuration = configuration;
        this.imapProperties = configService.createImapProperties();
        this.connectionPool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
        this.keepAliveExecutor = Executors.newScheduledThreadPool(2);

        log.info("{} Initializing IMAP Connection Manager with pool size: {}-{}",
                LogUtils.PROCESS_EMOJI, CORE_POOL_SIZE, MAX_POOL_SIZE);

        initializeConnectionPool();
        startKeepAlive();
    }

    public ImapConnection acquireConnection() throws MailConnectionException {
        try {
            ImapConnection connection = connectionPool.poll(5, TimeUnit.SECONDS);

            if (connection == null) {
                // Versuche neue Verbindung zu erstellen wenn Pool erschöpft
                if (totalConnectionsCreated.get() < MAX_POOL_SIZE) {
                    connection = createNewConnection();
                } else {
                    throw new MailConnectionException(
                            "No connections available in pool and maximum pool size reached",
                            MailConnectionException.ConnectionStage.POOL_EXHAUSTED,
                            null
                    );
                }
            }

            // Verbindung validieren und ggf. reconnecten
            if (!isConnectionHealthy(connection)) {
                log.debug("Connection {} is unhealthy, attempting reconnection", connection.getId());
                if (!reconnectConnection(connection)) {
                    invalidateConnection(connection);
                    return acquireConnection(); // Rekursiver Retry
                }
            }

            activeConnections.incrementAndGet();
            return connection;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MailConnectionException(
                    "Thread interrupted while acquiring connection",
                    MailConnectionException.ConnectionStage.POOL_ACQUISITION,
                    e
            );
        }
    }

    public void releaseConnection(ImapConnection connection) {
        if (connection != null) {
            activeConnections.decrementAndGet();

            if (isConnectionHealthy(connection)) {
                connectionPool.offer(connection);
            } else {
                log.debug("Released connection {} is unhealthy, not returning to pool", connection.getId());
                invalidateConnection(connection);
            }
        }
    }

    private void initializeConnectionPool() {
        log.info("{} Creating core connection pool of {} connections",
                LogUtils.PROCESS_EMOJI, CORE_POOL_SIZE);

        for (int i = 0; i < CORE_POOL_SIZE; i++) {
            try {
                ImapConnection connection = createNewConnection();
                connectionPool.offer(connection);
                log.debug("Created core connection {}/{}", i + 1, CORE_POOL_SIZE);
            } catch (Exception e) {
                log.error("{} Failed to create core connection {}: {}",
                        LogUtils.ERROR_EMOJI, i + 1, e.getMessage());
            }
        }

        log.info("{} Connection pool initialized with {} connections",
                LogUtils.SUCCESS_EMOJI, connectionPool.size());
    }

    private ImapConnection createNewConnection() throws MailConnectionException {
        int connectionId = totalConnectionsCreated.incrementAndGet();

        try {
            Store store = connectWithRetry();
            ImapConnection connection = new ImapConnection(store, connectionId);

            log.debug("Created new IMAP connection #{}", connectionId);
            return connection;

        } catch (Exception e) {
            totalConnectionsCreated.decrementAndGet();
            throw new MailConnectionException(
                    "Failed to create new IMAP connection #" + connectionId,
                    MailConnectionException.ConnectionStage.CONNECTION_CREATION,
                    e
            );
        }
    }

    private boolean isConnectionHealthy(ImapConnection connection) {
        try {
            if (connection == null || !connection.isConnected()) {
                return false;
            }

            // Schneller Health Check durch einfache Store-Operation
            Store store = connection.getStore();
            if (!store.isConnected()) {
                return false;
            }

            // Teste mit einfacher Folder-Operation
            store.getDefaultFolder();
            return true;

        } catch (Exception e) {
            log.debug("Connection {} health check failed: {}",
                    connection != null ? connection.getId() : "null", e.getMessage());
            return false;
        }
    }

    private boolean reconnectConnection(ImapConnection connection) {
        try {
            // Schließe alte Verbindung
            closeStoreSafely(connection.getStore());

            // Erstelle neue Verbindung
            Store newStore = connectWithRetry();
            connection.replaceStore(newStore);

            log.debug("Successfully reconnected connection {}", connection.getId());
            return true;

        } catch (Exception e) {
            log.debug("Failed to reconnect connection {}: {}", connection.getId(), e.getMessage());
            return false;
        }
    }

    private void startKeepAlive() {
        keepAliveExecutor.scheduleWithFixedDelay(
                this::performKeepAlive,
                KEEP_ALIVE_INTERVAL_SECONDS,
                KEEP_ALIVE_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        keepAliveExecutor.scheduleWithFixedDelay(
                this::maintainMinimumPoolSize,
                60,
                60,
                TimeUnit.SECONDS
        );
    }

    private void performKeepAlive() {
        try {
            connectionPool.forEach(connection -> {
                try {
                    if (isConnectionHealthy(connection)) {
                        connection.performKeepAlive();
                    } else {
                        log.debug("Removing unhealthy connection {} from pool", connection.getId());
                        connectionPool.remove(connection);
                        invalidateConnection(connection);
                    }
                } catch (Exception e) {
                    log.debug("Keep-alive failed for connection {}: {}", connection.getId(), e.getMessage());
                    connectionPool.remove(connection);
                    invalidateConnection(connection);
                }
            });

            logPoolStatistics();

        } catch (Exception e) {
            log.error("{} Keep-alive task failed: {}", LogUtils.ERROR_EMOJI, e.getMessage());
        }
    }

    private void maintainMinimumPoolSize() {
        try {
            int currentSize = connectionPool.size();
            int needed = CORE_POOL_SIZE - currentSize;

            if (needed > 0 && totalConnectionsCreated.get() < MAX_POOL_SIZE) {
                log.info("{} Pool below minimum size ({}/{}), creating {} connections",
                        LogUtils.PROCESS_EMOJI, currentSize, CORE_POOL_SIZE, needed);

                for (int i = 0; i < needed && totalConnectionsCreated.get() < MAX_POOL_SIZE; i++) {
                    try {
                        ImapConnection connection = createNewConnection();
                        connectionPool.offer(connection);
                        log.debug("Added connection {} to maintain minimum pool size", connection.getId());
                    } catch (Exception e) {
                        log.warn("Failed to create connection for pool maintenance: {}", e.getMessage());
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("{} Pool maintenance failed: {}", LogUtils.ERROR_EMOJI, e.getMessage());
        }
    }

    private void logPoolStatistics() {
        log.info("{} Connection Pool Stats: {} available, {} active, {} total created",
                LogUtils.CHART_EMOJI,
                connectionPool.size(),
                activeConnections.get(),
                totalConnectionsCreated.get());
    }

    private Store connectWithRetry() throws MailConnectionException {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                Session session = Session.getInstance(imapProperties);
                Store store = session.getStore("imaps");
                // Korrigiert: Verwende username statt email()
                store.connect(configuration.username(), configuration.password());

                log.debug("IMAP connection established on attempt {}", attempt);
                return store;

            } catch (Exception e) {
                lastException = e;
                log.warn("IMAP connection attempt {} failed: {}", attempt, e.getMessage());

                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(1000 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new MailConnectionException(
                                "Connection retry interrupted",
                                MailConnectionException.ConnectionStage.CONNECTION_RETRY,
                                ie
                        );
                    }
                }
            }
        }

        throw new MailConnectionException(
                String.format("Failed to establish IMAP connection after %d attempts", MAX_RETRY_ATTEMPTS),
                MailConnectionException.ConnectionStage.CONNECTION_ESTABLISHMENT,
                lastException
        );
    }

    public void shutdown() {
        log.info("{} Shutting down IMAP Connection Manager", LogUtils.STOP_EMOJI);

        keepAliveExecutor.shutdown();
        try {
            if (!keepAliveExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                keepAliveExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            keepAliveExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        connectionPool.forEach(this::closeConnectionSafely);
        connectionPool.clear();

        log.info("{} IMAP Connection Manager shutdown complete", LogUtils.SUCCESS_EMOJI);
    }

    private void closeConnectionSafely(ImapConnection connection) {
        try {
            if (connection != null) {
                closeStoreSafely(connection.getStore());
            }
        } catch (Exception e) {
            log.debug("Error closing connection {}: {}",
                    connection != null ? connection.getId() : "null", e.getMessage());
        }
    }

    private void closeStoreSafely(Store store) {
        try {
            if (store != null && store.isConnected()) {
                store.close();
            }
        } catch (Exception e) {
            log.debug("Error closing store: {}", e.getMessage());
        }
    }

    public void invalidateConnection(ImapConnection connection) {
        if (connection != null) {
            connectionPool.remove(connection);
            closeConnectionSafely(connection);
            log.debug("Invalidated connection {}", connection.getId());
        }
    }

    public int getAvailableConnections() {
        return connectionPool.size();
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public int getTotalConnectionsCreated() {
        return totalConnectionsCreated.get();
    }

    public int getMaxPoolSize() {
        return MAX_POOL_SIZE;
    }

    public int getCorePoolSize() {
        return CORE_POOL_SIZE;
    }

    public static class ImapConnection {
        private Store store;
        private final int id;
        private volatile long lastActivity;

        public ImapConnection(Store store, int id) {
            this.store = store;
            this.id = id;
            updateActivity();
        }

        public Store getStore() {
            updateActivity();
            return store;
        }

        public int getId() {
            return id;
        }

        void replaceStore(Store newStore) {
            this.store = newStore;
            updateActivity();
        }

        public boolean isConnected() {
            try {
                return store != null && store.isConnected();
            } catch (Exception e) {
                return false;
            }
        }

        public void performKeepAlive() throws MessagingException {
            if (store != null && store.isConnected()) {
                store.getDefaultFolder(); // Simple keep-alive operation
                updateActivity();
            }
        }

        private void updateActivity() {
            lastActivity = System.currentTimeMillis();
        }

        public long getLastActivity() {
            return lastActivity;
        }
    }
}