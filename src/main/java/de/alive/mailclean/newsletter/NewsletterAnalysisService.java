package de.alive.mailclean.newsletter;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.service.EmailAnalysisService;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
public class NewsletterAnalysisService implements EmailAnalysisService {

    private final NewsletterDetectionService detectionService;
    private final UnsubscribeLinkExtractor linkExtractor;
    private final NewsletterCacheService cacheService;

    public NewsletterAnalysisService() {
        this.detectionService = new NewsletterDetectionService();
        this.linkExtractor = new UnsubscribeLinkExtractor();
        this.cacheService = new NewsletterCacheService();
    }

    @Override
    public void analyzeEmail(Email email) {
        String normalizedSender = cacheService.normalizeSenderAddress(email.getSender());

        if (cacheService.isSenderKnown(normalizedSender)) {
            handleKnownSender(email, normalizedSender);
            return;
        }

        handleNewSender(email, normalizedSender);
    }

    private void handleKnownSender(Email email, String normalizedSender) {
        email.addMetadata("newsletter", true);
        email.addMetadata("skipped", true);
        email.addMetadata("cache_hit", true);
        email.addMetadata("normalized_sender", normalizedSender);

        cacheService.incrementSkippedEmails();
    }

    private void handleNewSender(Email email, String normalizedSender) {
        Object senderLock = cacheService.getSenderLock(normalizedSender);

        synchronized (senderLock) {
            if (cacheService.isSenderKnown(normalizedSender)) {
                handleKnownSender(email, normalizedSender);
                return;
            }

            boolean isNewsletter = detectionService.isNewsletterEmail(
                    email.getSubject(),
                    email.getSender(),
                    email.getContent()
            );

            email.addMetadata("newsletter", isNewsletter);
            email.addMetadata("normalized_sender", normalizedSender);

            if (isNewsletter) {
                Set<String> unsubscribeLinks = linkExtractor.extractUnsubscribeLinks(
                        email.getContent()
                );

                email.addMetadata("unsubscribe_links", unsubscribeLinks);

                if (!unsubscribeLinks.isEmpty()) {
                    cacheService.addSenderToCache(normalizedSender);
                    cacheService.addUnsubscribeLinks(unsubscribeLinks);
                    cacheService.incrementNewSenders();

                    log.info("🔗 {} unsubscribe links from NEW SENDER '{}' - ADDED TO CACHE",
                            unsubscribeLinks.size(), normalizedSender);
                }

                cacheService.incrementNewsletterEmails();
            }
        }

        cacheService.removeSenderLock(normalizedSender);
    }

    public int getNewsletterEmailsFound() { return cacheService.getNewsletterEmailsFound(); }
    public int getSkippedEmailsFromKnownSenders() { return cacheService.getSkippedEmailsFromKnownSenders(); }
    public int getNewSendersDiscovered() { return cacheService.getNewSendersDiscovered(); }
    public Set<String> getUnsubscribeLinks() { return cacheService.getAllUnsubscribeLinks(); }
    public Set<String> getSendersWithUnsubscribeLinks() { return cacheService.getCachedSenders(); }
}