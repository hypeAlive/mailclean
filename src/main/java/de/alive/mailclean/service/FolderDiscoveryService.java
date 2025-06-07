package de.alive.mailclean.service;

import de.alive.mailclean.domain.FolderInfo;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.exception.MailConnectionException;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.infrastructure.ImapConnectionManager.ImapConnection;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import javax.mail.Folder;
import javax.mail.MessagingException;
import javax.mail.Store;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FolderDiscoveryService {

    private final ImapConnectionManager connectionManager;
    private static final int DISCOVERY_TIMEOUT_SECONDS = 120;
    private static final int MAX_RETRY_ATTEMPTS = 3;

    public FolderDiscoveryService(ImapConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /**
     * Discovers folders from a list of folder names.
     * This method validates that the folders exist and analyzes them.
     */
    public List<FolderInfo> discoverFoldersFromNames(List<String> folderNames)
            throws MailConnectionException, EmailProcessingException {

        if (folderNames == null || folderNames.isEmpty()) {
            log.warn("{} No folder names provided, discovering all folders", LogUtils.WARNING_EMOJI);
            try {
                return discoverAllFolders();
            } catch (Exception e) {
                throw new EmailProcessingException(
                        "Failed to discover all folders",
                        "DISCOVERY_ALL",
                        "ALL_FOLDERS",
                        EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                        e
                );
            }
        }

        log.info("{} Discovering {} specific folders: {}",
                LogUtils.SEARCH_EMOJI, folderNames.size(), folderNames);

        List<FolderInfo> discoveredFolders = new ArrayList<>();

        for (String folderName : folderNames) {
            try {
                FolderInfo folderInfo = discoverSingleFolder(folderName);
                if (folderInfo != null && folderInfo.hasMessages()) {
                    discoveredFolders.add(folderInfo);
                    log.debug("{} Successfully discovered folder: {}",
                            LogUtils.SUCCESS_EMOJI, folderInfo.getDisplayInfo());
                } else {
                    log.warn("{} Folder '{}' has no messages or could not be analyzed",
                            LogUtils.WARNING_EMOJI, folderName);
                }
            } catch (Exception e) {
                log.error("{} Failed to discover folder '{}': {}",
                        LogUtils.ERROR_EMOJI, folderName, e.getMessage());
                // Continue with next folder instead of failing completely
            }
        }

        if (discoveredFolders.isEmpty()) {
            throw new EmailProcessingException(
                    "No valid folders could be discovered from the provided names: " + folderNames,
                    "NO_VALID_FOLDERS",
                    String.join(",", folderNames),
                    EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                    new IllegalStateException("All folder discoveries failed")
            );
        }

        log.info("{} Successfully discovered {} folders from {} names",
                LogUtils.SUCCESS_EMOJI, discoveredFolders.size(), folderNames.size());

        return sortFoldersByPriority(discoveredFolders);
    }

    public List<FolderInfo> discoverAllFolders() throws Exception {
        log.info("{} Starting folder discovery process...", LogUtils.SEARCH_EMOJI);
        long startTime = System.currentTimeMillis();

        try {
            List<FolderInfo> folderInfos = performDiscoveryWithTimeout();

            logDiscoveryResults(folderInfos, startTime);
            return sortFoldersByPriority(folderInfos);

        } catch (Exception e) {
            String duration = LogUtils.formatDurationMs(startTime);
            log.error("{} Folder discovery failed after {}: {}",
                    LogUtils.ERROR_EMOJI, duration, e.getMessage());

            if (e instanceof MailConnectionException) {
                throw e;
            }

            throw new EmailProcessingException(
                    "Folder discovery failed: " + e.getMessage(),
                    "DISCOVERY",
                    "ALL_FOLDERS",
                    EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                    e
            );
        }
    }

    // ... Rest der bestehenden Implementierung bleibt unverändert ...

    private List<FolderInfo> performDiscoveryWithTimeout() throws Exception {
        CompletableFuture<List<FolderInfo>> discoveryTask = CompletableFuture.supplyAsync(() -> {
            try {
                return performDiscoveryWithRetry();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            return discoveryTask.get(DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            discoveryTask.cancel(true);
            throw new EmailProcessingException(
                    "Folder discovery timeout after " + DISCOVERY_TIMEOUT_SECONDS + " seconds",
                    "DISCOVERY",
                    "ALL_FOLDERS",
                    EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                    e
            );
        }
    }

    private List<FolderInfo> performDiscoveryWithRetry() throws MailConnectionException, EmailProcessingException {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            ImapConnection connection = null;

            try {
                log.debug("{} Folder discovery attempt {}/{}",
                        LogUtils.PROCESS_EMOJI, attempt, MAX_RETRY_ATTEMPTS);

                connection = connectionManager.acquireConnection();
                List<FolderInfo> result = discoverFoldersFromConnection(connection);

                log.debug("{} Folder discovery attempt {} successful",
                        LogUtils.SUCCESS_EMOJI, attempt);
                return result;

            } catch (Exception e) {
                lastException = e;
                log.warn("{} Folder discovery attempt {}/{} failed: {}",
                        LogUtils.WARNING_EMOJI, attempt, MAX_RETRY_ATTEMPTS, e.getMessage());

                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(1000L * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new EmailProcessingException(
                                "Folder discovery interrupted",
                                "DISCOVERY",
                                "ALL_FOLDERS",
                                EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                                ie
                        );
                    }
                }

            } finally {
                if (connection != null) {
                    connectionManager.releaseConnection(connection);
                }
            }
        }

        throw new EmailProcessingException(
                String.format("Folder discovery failed after %d attempts", MAX_RETRY_ATTEMPTS),
                "DISCOVERY",
                "ALL_FOLDERS",
                EmailProcessingException.ProcessingStage.FOLDER_DISCOVERY,
                lastException
        );
    }

    private List<FolderInfo> discoverFoldersFromConnection(ImapConnection connection)
            throws MessagingException, EmailProcessingException {

        Store store = connection.getStore();
        if (!store.isConnected()) {
            throw new EmailProcessingException(
                    "IMAP connection not available for folder discovery",
                    "DISCOVERY",
                    "ALL_FOLDERS",
                    EmailProcessingException.ProcessingStage.CONNECTION_CHECK,
                    new IllegalStateException("Store not connected")
            );
        }

        List<FolderInfo> folderInfos = new ArrayList<>();

        try {
            Folder[] folders = store.getDefaultFolder().list("*");
            log.debug("{} Found {} potential folders to analyze",
                    LogUtils.SEARCH_EMOJI, folders.length);

            for (Folder folder : folders) {
                try {
                    FolderInfo folderInfo = analyzeSingleFolder(folder);
                    if (folderInfo != null && folderInfo.hasMessages()) {
                        folderInfos.add(folderInfo);
                    }
                } catch (Exception e) {
                    log.debug("Skipping folder '{}': {}", folder.getFullName(), e.getMessage());
                }
            }

        } catch (MessagingException e) {
            throw new EmailProcessingException(
                    "Failed to list folders from IMAP server",
                    "DISCOVERY",
                    "ALL_FOLDERS",
                    EmailProcessingException.ProcessingStage.FOLDER_LISTING,
                    e
            );
        }

        return folderInfos;
    }

    private FolderInfo analyzeSingleFolder(Folder folder) throws MessagingException {
        String folderName = folder.getFullName();

        try {
            if (!canOpenFolder(folder)) {
                log.debug("Cannot open folder '{}' - skipping", folderName);
                return null;
            }

            folder.open(Folder.READ_ONLY);
            int messageCount = folder.getMessageCount();

            log.debug("{} Folder '{}': {} messages ({})",
                    LogUtils.EMAIL_EMOJI, folderName, messageCount,
                    messageCount > 0 ? "INCLUDED" : "EMPTY");

            return new FolderInfo(folderName, messageCount);

        } finally {
            try {
                if (folder.isOpen()) {
                    folder.close();
                }
            } catch (Exception e) {
                log.debug("Error closing folder '{}': {}", folderName, e.getMessage());
            }
        }
    }

    private boolean canOpenFolder(Folder folder) {
        try {
            return (folder.getType() & Folder.HOLDS_MESSAGES) != 0;
        } catch (MessagingException e) {
            log.debug("Cannot determine folder type for '{}': {}", folder.getFullName(), e.getMessage());
            return false;
        }
    }

    private List<FolderInfo> sortFoldersByPriority(List<FolderInfo> folderInfos) {
        return folderInfos.stream()
                .sorted(Comparator
                        .comparingInt(FolderInfo::getPriority)
                        .thenComparing(FolderInfo::messageCount, Comparator.reverseOrder())
                        .thenComparing(FolderInfo::folderName))
                .toList();
    }

    private void logDiscoveryResults(List<FolderInfo> folderInfos, long startTime) {
        String duration = LogUtils.formatDurationMs(startTime);
        int totalMessages = folderInfos.stream().mapToInt(FolderInfo::messageCount).sum();
        int totalBatches = folderInfos.stream().mapToInt(FolderInfo::estimatedBatches).sum();

        log.info("{} Folder discovery completed in {}", LogUtils.SUCCESS_EMOJI, duration);
        log.info("{} Summary: {} folders, {:,} total emails, ~{} batches",
                LogUtils.CHART_EMOJI, folderInfos.size(), totalMessages, totalBatches);

        if (!folderInfos.isEmpty()) {
            log.info("");
            log.info("{} DISCOVERED FOLDERS (by priority):", LogUtils.FOLDER_EMOJI);
            log.info("═══════════════════════════════════════════════════");

            folderInfos.forEach(folder -> {
                log.info("   {} {}",
                        getFolderEmoji(folder.type()),
                        folder.getDisplayInfo());
            });

            log.info("═══════════════════════════════════════════════════");

            // Additional insights
            logFolderInsights(folderInfos, totalMessages);
        } else {
            log.warn("{} No folders with messages found!", LogUtils.WARNING_EMOJI);
        }
    }

    private void logFolderInsights(List<FolderInfo> folderInfos, int totalMessages) {
        // High volume folders
        var highVolumeFolders = folderInfos.stream()
                .filter(FolderInfo::isHighVolume)
                .toList();

        if (!highVolumeFolders.isEmpty()) {
            log.info("");
            log.info("{} High-volume folders (>10K messages):", LogUtils.WARNING_EMOJI);
            highVolumeFolders.forEach(folder ->
                    log.info("   {} {} - {:.1f} min estimated processing time",
                            LogUtils.SEARCH_EMOJI, folder.folderName(),
                            folder.getEstimatedProcessingTimeMinutes()));
        }

        // Processing time estimate
        double totalMinutes = folderInfos.stream()
                .mapToDouble(FolderInfo::getEstimatedProcessingTimeMinutes)
                .sum();

        log.info("");
        log.info("{} Estimated total processing time: {}",
                LogUtils.SEARCH_EMOJI, LogUtils.formatDurationMs((long)(totalMinutes * 60 * 1000)));
    }

    private String getFolderEmoji(FolderInfo.FolderType type) {
        return switch (type) {
            case INBOX -> "📥";
            case SENT -> "📤";
            case DRAFTS -> "📝";
            case SPAM -> "🚫";
            case TRASH -> "🗑️";
            case ARCHIVE -> "📦";
            case OTHER -> "📁";
        };
    }

    public FolderInfo discoverSingleFolder(String folderName) throws MailConnectionException, EmailProcessingException {
        log.debug("{} Discovering single folder: {}", LogUtils.SEARCH_EMOJI, folderName);

        ImapConnection connection = null;
        try {
            connection = connectionManager.acquireConnection();
            Store store = connection.getStore();

            Folder folder = store.getFolder(folderName);
            if (!folder.exists()) {
                throw new EmailProcessingException(
                        "Folder does not exist: " + folderName,
                        "DISCOVERY",
                        folderName,
                        EmailProcessingException.ProcessingStage.FOLDER_ACCESS,
                        new IllegalArgumentException("Folder not found")
                );
            }

            return analyzeSingleFolder(folder);

        } catch (MessagingException e) {
            throw new EmailProcessingException(
                    "Failed to discover folder: " + folderName,
                    "DISCOVERY",
                    folderName,
                    EmailProcessingException.ProcessingStage.FOLDER_ACCESS,
                    e
            );
        } finally {
            if (connection != null) {
                connectionManager.releaseConnection(connection);
            }
        }
    }
}