package de.alive.mailclean;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.service.EmailAnalysisService;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class EmailCollector {

    private final EmailCollectionOrchestrator orchestrator;

    public EmailCollector(Configuration configuration, EmailAnalysisService analysisService) {
        this.orchestrator = new EmailCollectionOrchestrator(configuration, analysisService);
    }

    public Mono<Integer> start() {
        return orchestrator.start();
    }

    public Flux<Email> getEmailStream() {
        return orchestrator.getEmailStream();
    }

    public int getTotalEmitted() {
        return orchestrator.getTotalEmitted();
    }

    public void shutdown() {
        orchestrator.shutdown();
    }
}