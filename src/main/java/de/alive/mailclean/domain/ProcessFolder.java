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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.mail.*;

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
        @NotNull
        public Flux<BatchEntity> entities(@NotNull IConnection connection) {
            return openFolder(connection)
                    .flatMap(this::getMessagesFromFolder)
                    .flatMapMany(messages -> Flux.fromArray(messages)
                            .flatMap(this::createEmailFromMessage)
                            .onErrorResume(error -> {
                                log.warn("Skipping message due to error: {}", error.getMessage());
                                return Mono.empty();
                            }));
        }

        @NotNull
        private Mono<Folder> openFolder(@NotNull IConnection connection) {
            return Mono.fromCallable(() -> {
                        ImapConnectionManager.Connection imapConn = (ImapConnectionManager.Connection) connection;
                        Store store = imapConn.getStore();

                        Folder workingFolder = store.getFolder(folder().getFolder().getFullName());

                        workingFolder.open(Folder.READ_ONLY);
                        return workingFolder;
                    })
                    .subscribeOn(Schedulers.boundedElastic());
        }

        @NotNull
        private Mono<Message[]> getMessagesFromFolder(@NotNull Folder workingFolder) {
            return Mono.fromCallable(() -> {
                        int messageCount = workingFolder.getMessageCount();

                        if (start > messageCount) {
                            log.warn("Batch start {} exceeds message count {}", start, messageCount);
                            return new Message[0];
                        }

                        int adjustedEnd = Math.min(end, messageCount);
                        if (end > messageCount) {
                            log.warn("Batch end {} exceeds message count {}, adjusting to {}",
                                    end, messageCount, adjustedEnd);
                        }

                        Message[] messages = workingFolder.getMessages(start, adjustedEnd);

                        FetchProfile fetchProfile = new FetchProfile();
                        fetchProfile.add(FetchProfile.Item.ENVELOPE);
                        fetchProfile.add(UIDFolder.FetchProfileItem.UID);
                        workingFolder.fetch(messages, fetchProfile);

                        return messages;
                    })
                    .doFinally(signalType -> closeFolder(workingFolder))
                    .subscribeOn(Schedulers.boundedElastic());
        }


        @NotNull
        private Mono<BatchEntity> createEmailFromMessage(@NotNull Message message) {
            return Mono.fromCallable(() -> {
                        String subject = message.getSubject();
                        return (BatchEntity) new Email(
                                message.getMessageNumber(),
                                subject,
                                folder()
                        );
                    })
                    .onErrorResume(error -> Mono.empty())
                    .subscribeOn(Schedulers.boundedElastic());
        }

        private void closeFolder(@Nullable Folder workingFolder) {
            if (workingFolder != null && workingFolder.isOpen()) {
                Mono.fromCallable(() -> {
                            workingFolder.close(false);
                            return null;
                        })
                        .onErrorResume(MessagingException.class, e -> {
                            log.warn("Failed to close folder {}: {}",
                                    workingFolder.getFullName(), e.getMessage());
                            return Mono.empty();
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe();
            }
        }

    }
}