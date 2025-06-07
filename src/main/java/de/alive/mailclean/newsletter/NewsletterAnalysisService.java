package de.alive.mailclean.newsletter;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.service.EmailAnalysisService;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class NewsletterAnalysisService implements EmailAnalysisService {

    private final NewsletterDetectionService detectionService;
    private final UnsubscribeLinkExtractor linkExtractor;
    private final NewsletterCacheService cacheService;

    // Statistics tracking
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
    private final LongAdder totalAnalyzed = new LongAdder();
    private final LongAdder totalErrors = new LongAdder();
    private final LongAdder analysisTimeMs = new LongAdder();
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder newSenders = new LongAdder();

    public NewsletterAnalysisService() {
        this.detectionService = new NewsletterDetectionService();
        this.linkExtractor = new UnsubscribeLinkExtractor();
        this.cacheService = new NewsletterCacheService();

        log.info("{} Newsletter Analysis Service initialized", LogUtils.SUCCESS_EMOJI);
    }

    @Override
    public void analyzeEmail(Email email) throws EmailProcessingException {
        long startTime = System.nanoTime();

        try {
            validateEmail(email);

            String normalizedSender = cacheService.normalizeSenderAddress(email.getSender());
            email.addMetadata("normalized_sender", normalizedSender);

            if (cacheService.isSenderKnown(normalizedSender)) {
                handleKnownSender(email, normalizedSender);
                cacheHits.increment();
            } else {
                handleNewSender(email, normalizedSender);
            }

            totalAnalyzed.increment();

        } catch (Exception e) {
            totalErrors.increment();

            // Add error metadata to email for debugging
            email.addMetadata("analysis_error", e.getMessage());
            email.addMetadata("analysis_failed", true);

            if (e instanceof EmailProcessingException) {
                throw e;
            }

            throw new EmailProcessingException(
                    "Newsletter analysis failed: " + e.getMessage(),
                    "ANALYSIS",
                    email.getFolder(),
                    EmailProcessingException.ProcessingStage.EMAIL_ANALYSIS,
                    e
            );
        } finally {
            long elapsedNanos = System.nanoTime() - startTime;
            analysisTimeMs.add(elapsedNanos / 1_000_000);
        }
    }

    private void validateEmail(Email email) throws EmailProcessingException {
        if (email == null) {
            throw new EmailProcessingException(
                    "Email cannot be null",
                    "ANALYSIS",
                    "UNKNOWN",
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Null email")
            );
        }

        if (email.getSender() == null || email.getSender().trim().isEmpty()) {
            throw new EmailProcessingException(
                    "Email sender cannot be null or empty",
                    "ANALYSIS",
                    email.getFolder(),
                    EmailProcessingException.ProcessingStage.VALIDATION,
                    new IllegalArgumentException("Invalid sender")
            );
        }
    }

    private void handleKnownSender(Email email, String normalizedSender) {
        email.addMetadata("newsletter", true);
        email.addMetadata("skipped", true);
        email.addMetadata("cache_hit", true);
        email.addMetadata("analysis_type", "CACHED");

        cacheService.incrementSkippedEmails();

        log.debug("{} Cache hit for sender: {}", LogUtils.SEARCH_EMOJI, normalizedSender);
    }

    private void handleNewSender(Email email, String normalizedSender) {
        Object senderLock = cacheService.getSenderLock(normalizedSender);

        synchronized (senderLock) {
            // Double-check pattern - sender might have been added while waiting for lock
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
            email.addMetadata("analysis_type", "FULL_ANALYSIS");
            email.addMetadata("cache_hit", false);

            if (isNewsletter) {
                processNewsletterEmail(email, normalizedSender);
            } else {
                email.addMetadata("newsletter_reason", "Not detected as newsletter");
            }
        }

        cacheService.removeSenderLock(normalizedSender);
    }

    private void processNewsletterEmail(Email email, String normalizedSender) {
        Set<String> unsubscribeLinks = linkExtractor.extractUnsubscribeLinks(email.getContent());
        email.addMetadata("unsubscribe_links", unsubscribeLinks);
        email.addMetadata("unsubscribe_count", unsubscribeLinks.size());

        if (!unsubscribeLinks.isEmpty()) {
            cacheService.addSenderToCache(normalizedSender);
            cacheService.addUnsubscribeLinks(unsubscribeLinks);
            newSenders.increment();

            log.info("{} {} unsubscribe links from NEW SENDER '{}' - ADDED TO CACHE",
                    LogUtils.SEARCH_EMOJI, unsubscribeLinks.size(), normalizedSender);
        } else {
            email.addMetadata("newsletter_reason", "Newsletter without unsubscribe links");
            log.debug("{} Newsletter without unsubscribe links: {}",
                    LogUtils.WARNING_EMOJI, normalizedSender);
        }

        cacheService.incrementNewsletterEmails();
    }

    @Override
    public boolean canAnalyze(Email email) {
        return email != null &&
                email.getSender() != null &&
                !email.getSender().trim().isEmpty() &&
                email.getContent() != null;
    }

    @Override
    public int getPriority() {
        return 10; // High priority for newsletter detection
    }

    @Override
    public String getDescription() {
        return "Analyzes emails for newsletter patterns and extracts unsubscribe links";
    }

    @Override
    public AnalysisStatistics getStatistics() {
        return new NewsletterAnalysisStatistics();
    }

    @Override
    public void reset() {
        totalAnalyzed.reset();
        totalErrors.reset();
        analysisTimeMs.reset();
        cacheHits.reset();
        newSenders.reset();
        startTime.set(System.currentTimeMillis());

        if (cacheService != null) {
            cacheService.clearCache();
        }

        log.info("{} Newsletter Analysis Service reset", LogUtils.SEARCH_EMOJI);
    }

    @Override
    public void shutdown() {
        log.info("{} Shutting down Newsletter Analysis Service...", LogUtils.STOP_EMOJI);

        // Log final statistics
        AnalysisStatistics finalStats = getStatistics();
        log.info("{} Final Statistics: {}", LogUtils.CHART_EMOJI, finalStats.getSummary());

        // Additional newsletter-specific stats
        log.info("{} Cache Performance: {}/{} hits ({:.1f%})",
                LogUtils.SEARCH_EMOJI,
                cacheHits.sum(), totalAnalyzed.sum(),
                totalAnalyzed.sum() > 0 ? (cacheHits.sum() * 100.0) / totalAnalyzed.sum() : 0.0);

        log.info("{} New Senders Discovered: {}", LogUtils.SEARCH_EMOJI, newSenders.sum());

        if (cacheService != null) {
            log.info("{} Total Cached Senders: {}", LogUtils.SEARCH_EMOJI, cacheService.getCachedSenders().size());
            log.info("{} Total Unsubscribe Links: {}", LogUtils.SEARCH_EMOJI, cacheService.getAllUnsubscribeLinks().size());
        }
    }

    // Legacy compatibility methods
    public int getNewsletterEmailsFound() {
        return cacheService != null ? cacheService.getNewsletterEmailsFound() : 0;
    }

    public int getSkippedEmailsFromKnownSenders() {
        return cacheService != null ? cacheService.getSkippedEmailsFromKnownSenders() : 0;
    }

    public int getNewSendersDiscovered() {
        return (int) newSenders.sum();
    }

    public Set<String> getUnsubscribeLinks() {
        return cacheService != null ? cacheService.getAllUnsubscribeLinks() : Set.of();
    }

    public Set<String> getSendersWithUnsubscribeLinks() {
        return cacheService != null ? cacheService.getCachedSenders() : Set.of();
    }

    private class NewsletterAnalysisStatistics implements AnalysisStatistics {

        @Override
        public long getTotalAnalyzed() {
            return totalAnalyzed.sum();
        }

        @Override
        public long getTotalErrors() {
            return totalErrors.sum();
        }

        @Override
        public long getAnalysisTimeMs() {
            return analysisTimeMs.sum();
        }

        @Override
        public String getSummary() {
            return String.format("Newsletter Analysis: %s | Cache Hits: %,d | New Senders: %,d",
                    AnalysisStatistics.super.getSummary(),
                    cacheHits.sum(),
                    newSenders.sum());
        }
    }
}