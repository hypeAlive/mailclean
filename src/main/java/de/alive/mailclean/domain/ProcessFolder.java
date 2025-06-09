package de.alive.mailclean.domain;

import de.alive.mailclean.exception.InvalidFolderException;
import de.alive.mailclean.infrastructure.IConnection;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.service.Batch;
import de.alive.mailclean.service.BatchEntity;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.mail.Folder;
import javax.mail.MessagingException;
import javax.mail.Store;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Getter
public class ProcessFolder {

    @NotNull
    private final Folder folder;
    @Nullable
    private final ProcessingTracker processingTracker;

    private ProcessFolder(@NotNull Folder folder, @Nullable ProcessingTracker processingTracker) {
        this.folder = folder;
        this.processingTracker = processingTracker;
    }

    public ProcessFolder(@NotNull Folder folder) throws MessagingException {
        this(folder, new ProcessingStatistics(folder.getMessageCount()));
    }

    @NotNull
    public MailBatch getBatch(int start, int end) {
        return new MailBatch(this, start, end);
    }

    public record MailBatch(@NotNull ProcessFolder folder, int start, int end) implements Batch {
        public MailBatch {
            if (start <= 0 || end <= 0 || start > end)
                throw InvalidFolderException.invalidBatchRange(this);
        }

        public int size() {
            return end - start + 1;
        }

        @Override
        public List<BatchEntity> entities(IConnection connection) {
            log.info("Get Entities with connection {}",
                    connection instanceof ImapConnectionManager.Connection ?
                            ((ImapConnectionManager.Connection) connection).getId() : "unknown");

            Folder workingFolder = null;
            try {
                // ✅ Verwende die Connection um den Folder zu öffnen
                assert connection instanceof ImapConnectionManager.Connection;
                Store store = ((ImapConnectionManager.Connection) connection).getStore();

                // ✅ Check store connection before use
                if (!store.isConnected()) {
                    throw new MessagingException("Store is not connected");
                }

                workingFolder = store.getFolder(folder().getFolder().getFullName());

                // ✅ Check if folder exists before opening
                if (!workingFolder.exists()) {
                    throw new MessagingException("Folder does not exist: " + workingFolder.getFullName());
                }

                workingFolder.open(Folder.READ_ONLY);

                // ✅ Validate message range
                int messageCount = workingFolder.getMessageCount();
                if (end > messageCount) {
                    log.warn("Batch end {} exceeds message count {}, adjusting", end, messageCount);
                }

                if (start > messageCount) {
                    log.warn("Batch start {} exceeds message count {}, returning empty", start, messageCount);
                    return List.of();
                }

                Folder finalWorkingFolder = workingFolder;
                return Arrays.stream(workingFolder.getMessages(start, Math.min(end, messageCount)))
                        .map(message -> {
                            try {
                                return (BatchEntity) new Email(
                                        message.getMessageNumber(),
                                        message.getSubject() != null ? message.getSubject() : "No Subject",
                                        folder()
                                );
                            } catch (MessagingException e) {
                                log.error("Failed to get message {} from folder: {}",
                                        message.getMessageNumber(), finalWorkingFolder.getFullName(), e);
                                // ✅ Return placeholder instead of throwing
                                return (BatchEntity) new Email(
                                        message.getMessageNumber(),
                                        "Error reading message",
                                        folder()
                                );
                            }
                        }).toList();

            } catch (MessagingException e) {
                log.error("Failed to get messages from folder: {}", folder().getFolder().getFullName(), e);
                throw new RuntimeException("Failed to get messages from folder: " + e.getMessage(), e);
            } finally {
                // ✅ ALWAYS close folder to prevent resource leaks!
                if (workingFolder != null && workingFolder.isOpen()) {
                    try {
                        workingFolder.close(false); // false = don't expunge
                        log.debug("Folder {} closed successfully", workingFolder.getFullName());
                    } catch (MessagingException e) {
                        log.warn("Failed to close folder {}: {}", workingFolder.getFullName(), e.getMessage());
                    }
                }
            }
        }
    }
}