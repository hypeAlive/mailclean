package de.alive.mailclean.infrastructure;

import de.alive.mailclean.Configuration;
import lombok.extern.slf4j.Slf4j;

import javax.mail.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ImapConnectionManager {

    private final Configuration configuration;
    private final Properties imapProperties;
    private final List<Store> activeStores = new ArrayList<>();
    private final Object storesLock = new Object();
    private ScheduledExecutorService keepAliveExecutor;

    public ImapConnectionManager(Configuration configuration) {
        this.configuration = configuration;
        this.imapProperties = createImapProperties();
    }

    public void startKeepAlive() {
        keepAliveExecutor = Executors.newScheduledThreadPool(1);
        keepAliveExecutor.scheduleAtFixedRate(this::keepConnectionsAlive, 30, 30, TimeUnit.SECONDS);
    }

    public ImapConnection createConnection(int threadId, int connectionId) {
        try {
            long staggerDelay = (threadId * 500L) + (connectionId * 200L);
            if (staggerDelay > 0) {
                Thread.sleep(staggerDelay);
            }

            Store store = connectWithRetry();
            if (store != null) {
                synchronized (storesLock) {
                    activeStores.add(store);
                }
                return new ImapConnection(store, connectionId);
            }

        } catch (Exception e) {
            log.warn("Failed to create connection for thread {} conn {}: {}", threadId, connectionId, e.getMessage());
        }

        return null;
    }

    public boolean reconnectConnection(ImapConnection connection, int threadId) {
        try {
            Store oldStore = connection.getStore();

            try {
                if (oldStore.isConnected()) {
                    oldStore.close();
                }
            } catch (Exception e) {
                log.debug("Error closing old store: {}", e.getMessage());
            }

            synchronized (storesLock) {
                activeStores.remove(oldStore);
            }

            Store newStore = connectWithRetry();
            if (newStore != null) {
                connection.replaceStore(newStore);

                synchronized (storesLock) {
                    activeStores.add(newStore);
                }

                log.info("✅ Thread {} Conn {}: Successfully reconnected", threadId, connection.getId());
                return true;
            }

        } catch (Exception e) {
            log.warn("❌ Thread {} Conn {}: Reconnection failed: {}", threadId, connection.getId(), e.getMessage());
        }

        return false;
    }

    public void shutdown() {
        log.info("🛑 Shutting down connection manager...");

        if (keepAliveExecutor != null) {
            keepAliveExecutor.shutdown();
        }

        synchronized (storesLock) {
            log.info("🔌 Closing {} active IMAP connections...", activeStores.size());
            for (Store store : activeStores) {
                try {
                    if (store.isConnected()) {
                        store.close();
                    }
                } catch (Exception e) {
                    log.debug("Error closing connection: {}", e.getMessage());
                }
            }
            activeStores.clear();
        }

        log.info("✅ Connection manager shutdown completed");
    }

    private void keepConnectionsAlive() {
        synchronized (storesLock) {
            int alive = 0;
            List<Store> deadStores = new ArrayList<>();

            for (Store store : activeStores) {
                try {
                    if (store.isConnected()) {
                        Folder inbox = store.getFolder("INBOX");
                        if (!inbox.isOpen()) {
                            inbox.open(Folder.READ_ONLY);
                        }
                        inbox.getMessageCount(); // Keep-Alive ping
                        inbox.close();
                        alive++;
                    } else {
                        deadStores.add(store);
                    }
                } catch (Exception e) {
                    log.debug("Keep-Alive error for store: {}", e.getMessage());
                    deadStores.add(store);
                }
            }

            if (!deadStores.isEmpty()) {
                activeStores.removeAll(deadStores);
                log.debug("🗑️ Removed {} dead connections from active list", deadStores.size());
            }

            log.debug("🔄 Keep-Alive: {}/{} connections active", alive, activeStores.size());
        }
    }

    private Store connectWithRetry() throws MessagingException {
        Session session = Session.getInstance(imapProperties);
        Store store = session.getStore("imaps");

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                store.connect(configuration.username(), configuration.password());
                log.debug("✅ IMAP connection established (attempt {})", attempt);
                return store;
            } catch (MessagingException e) {
                log.warn("⚠️ IMAP connection attempt {} failed: {}", attempt, e.getMessage());
                if (attempt == 3) throw e;

                try {
                    Thread.sleep(1000 * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MessagingException("Connection interrupted", ie);
                }
            }
        }

        throw new MessagingException("Failed to connect after 3 attempts");
    }

    private Properties createImapProperties() {
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.host", "imap.gmail.com");
        props.setProperty("mail.imaps.port", "993");
        props.setProperty("mail.imaps.ssl.enable", "true");
        props.setProperty("mail.imaps.ssl.trust", "*");
        props.setProperty("mail.imaps.connectionpoolsize", "20");
        props.setProperty("mail.imaps.connectionpooltimeout", "300000");
        props.setProperty("mail.imaps.connectiontimeout", "10000");
        props.setProperty("mail.imaps.timeout", "10000");
        return props;
    }

    public static class ImapConnection {
        private Store store;
        private final int id;

        public ImapConnection(Store store, int id) {
            this.store = store;
            this.id = id;
        }

        public Store getStore() { return store; }
        public int getId() { return id; }

        void replaceStore(Store newStore) {
            this.store = newStore;
        }

        public boolean isConnected() {
            return store != null && store.isConnected();
        }
    }
}