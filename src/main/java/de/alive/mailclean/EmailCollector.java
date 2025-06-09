package de.alive.mailclean;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.domain.ProcessFolder;
import de.alive.mailclean.domain.ProcessingTracker;
import de.alive.mailclean.infrastructure.BatchProcessor;
import de.alive.mailclean.infrastructure.IConnection;
import de.alive.mailclean.service.Batch;
import de.alive.mailclean.service.BatchEntity;
import de.alive.mailclean.service.MailAnalyser;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.mail.MessagingException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EmailCollector {

    private static final int BATCH_SIZE = 50;

    @NotNull
    private final List<ProcessFolder> mailFolders;
    @NotNull
    private final MailAnalyser analyser;
    @NotNull
    private final BatchProcessor processor;

    public EmailCollector(@NotNull MailAnalyser analyser, @NotNull BatchProcessor processor, @NotNull List<ProcessFolder> folders) {
        this.mailFolders = folders;
        this.analyser = analyser;
        this.processor = processor;
    }

    public Flux<Email> getMailStream() {
        return Mono.fromCallable(this::createBatches)
                .flatMapMany(batches -> processor.processAllBatches(batches, (batch, con) -> {
                    return batch;
                }))
                .map(batch -> (Email) batch)
                .onErrorMap(MessagingException.class, ex -> {
                    log.error("Error processing mail stream", ex);
                    return new RuntimeException("Failed to process mail stream", ex);
                });
    }

    private List<Batch> createBatches() throws MessagingException {
        List<Batch> batches = new ArrayList<>();

        for (ProcessFolder pfolder : this.mailFolders) {
            int msgCount = pfolder.getFolder().getMessageCount();
            for (int pos = 1; pos <= msgCount; pos += BATCH_SIZE) {
                int endPos = Math.min(pos + BATCH_SIZE - 1, msgCount);
                batches.add(pfolder.getBatch(pos, endPos));
            }
        }

        log.info("🔥 Created: {} batches for {} folders", batches.size(), mailFolders.size());
        return batches;
    }

}