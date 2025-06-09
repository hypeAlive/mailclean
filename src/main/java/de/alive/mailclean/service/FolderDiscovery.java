package de.alive.mailclean.service;

import de.alive.mailclean.domain.ProcessFolder;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.mail.Folder;
import javax.mail.Store;
import java.time.Duration;
import java.util.List;

@Slf4j
public class FolderDiscovery {

    @NotNull
    private final ImapConnectionManager connectionManager;

    public FolderDiscovery(@NotNull ImapConnectionManager manager) {
        this.connectionManager = manager;
    }

    @NotNull
    public Mono<List<ProcessFolder>> discoverAllFolders() {
        return connectionManager.createConnection()
                .flatMap(connection -> Mono
                        .fromCallable(() -> {
                            Store store = ((ImapConnectionManager.Connection) connection).getStore();
                            Folder[] folders = store.getDefaultFolder().list("*");
                            return folders;
                        })
                        .flatMapMany(Flux::fromArray)
                        .flatMap(folder -> Mono
                                .fromCallable(() -> {
                                    folder.open(Folder.READ_ONLY);
                                    return new ProcessFolder(folder);
                                })
                                .onErrorResume(ignore -> {
                                    log.warn("{} Mail folder not found: {}", LogUtils.WARNING_EMOJI, folder.getFullName());
                                    return Mono.empty();
                                })
                                .doOnNext(mailFolder ->
                                        log.info("{} Found mail folder: {}", LogUtils.SUCCESS_EMOJI, mailFolder.getFolder().getFullName())
                                )
                        )
                        .collectList()
                        .flatMap(folders ->
                                connectionManager.closeConnection(connection)
                                        .doOnNext(closed -> {
                                            if (closed) {
                                                log.debug("{} Discovery connection closed successfully", LogUtils.SUCCESS_EMOJI);
                                            } else {
                                                log.warn("{} Failed to close discovery connection", LogUtils.WARNING_EMOJI);
                                            }
                                        })
                                        .thenReturn(folders)
                        )
                )
                .publishOn(Schedulers.boundedElastic())
                .doOnError(error -> {
                    log.error("{} Folder discovery failed: {}", LogUtils.ERROR_EMOJI, error.getMessage());
                })
                .doOnSuccess(folders ->
                        log.info("{} Discovered {} mail folders", LogUtils.SUCCESS_EMOJI, folders.size())
                )
                .timeout(Duration.ofSeconds(300));
    }
}