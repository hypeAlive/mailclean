package de.alive.mailclean;

import de.alive.mailclean.infrastructure.BatchProcessor;
import de.alive.mailclean.infrastructure.ImapConnectionManager;

import de.alive.mailclean.newsletter.NewsletterMailAnalyser;
import de.alive.mailclean.service.FolderDiscovery;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {

    private static final String EMAIL = System.getenv("EMAIL");
    private static final String APP_PASSWORD = System.getenv("APP_PASSWORD");

    public static void main(String[] args) {
        Configuration configuration = new Configuration(EMAIL, APP_PASSWORD);
        ImapConnectionManager manager = new ImapConnectionManager(configuration);
        FolderDiscovery folderDiscovery = new FolderDiscovery(manager);
        NewsletterMailAnalyser analyser = new NewsletterMailAnalyser();
        BatchProcessor processor = new BatchProcessor(manager, 7);
        folderDiscovery.discoverAllFolders()
                .flatMapMany(folders -> new EmailCollector(analyser, processor, folders).getMailStream())
                .doOnNext(entity -> {
                    log.info("Processed Mail");
                })
                .doOnComplete(() -> log.info("All emails processed"))
                .doOnError(error -> log.error("Error in email stream: {}", error.getMessage()))
                .blockLast();

    }

}