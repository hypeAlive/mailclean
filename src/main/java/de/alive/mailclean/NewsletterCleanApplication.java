package de.alive.mailclean;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.newsletter.NewsletterAnalysisService;
import de.alive.mailclean.newsletter.NewsletterReportingService;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class NewsletterCleanApplication {

    private final EmailCollector emailCollector;
    private final NewsletterAnalysisService newsletterAnalysisService;
    private final NewsletterReportingService reportingService;

    public NewsletterCleanApplication() {
        String email = System.getenv("EMAIL");
        String appPassword = System.getenv("APP_PASSWORD");

        if (email == null || appPassword == null) {
            throw new IllegalStateException("EMAIL and APP_PASSWORD environment variables must be set");
        }

        Configuration configuration = new Configuration(email, appPassword);
        this.newsletterAnalysisService = new NewsletterAnalysisService();
        this.emailCollector = new EmailCollector(configuration, newsletterAnalysisService);
        this.reportingService = new NewsletterReportingService();
    }

    public static void main(String[] args) {
        log.info("🚀 Starting Newsletter Analysis Application...");

        NewsletterCleanApplication app = new NewsletterCleanApplication();
        try {
            app.run();
        } catch (Exception e) {
            log.error("❌ Application failed: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    public void run() {
        CountDownLatch processingCompleted = new CountDownLatch(1);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        try {
            emailCollector.getEmailStream()
                    .timeout(Duration.ofSeconds(600))
                    .onErrorContinue((error, item) -> {
                        int errors = errorCount.incrementAndGet();
                        if (errors % 100 == 0) {
                            log.warn("⚠️ Processing error #{}: {}", errors, error.getMessage());
                        }
                    })
                    .parallel(8)
                    .runOn(Schedulers.parallel())
                    .doOnNext(email -> {
                        int count = processedCount.incrementAndGet();
                        processEmail(email, count);
                    })
                    .sequential()
                    .doOnComplete(() -> {
                        log.info("🏁 Email stream completed!");
                        processingCompleted.countDown();
                    })
                    .doOnError(error -> {
                        log.error("❌ Stream error: {}", error.getMessage());
                        processingCompleted.countDown();
                    })
                    .subscribe();

            Integer totalEmails = emailCollector.start().block();
            log.info("✅ Email collection completed! {} emails collected", totalEmails);

            boolean completed = processingCompleted.await(40, TimeUnit.MINUTES);

            if (!completed) {
                log.warn("⏰ Processing timeout - generating report with current data");
            } else {
                log.info("✅ All processing completed successfully!");
            }

            generateReports();

        } catch (Exception e) {
            log.error("❌ Error during execution: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            emailCollector.shutdown();
            log.info("✅ Application shutdown completed");
        }
    }

    private void processEmail(Email email, int count) {
        if (count % 1000 == 0) {
            log.info("📊 Processed: {} emails - {} newsletters - {} unsubscribe links - {} skipped",
                    count,
                    newsletterAnalysisService.getNewsletterEmailsFound(),
                    newsletterAnalysisService.getUnsubscribeLinks().size(),
                    newsletterAnalysisService.getSkippedEmailsFromKnownSenders());
        }

        Boolean isNewsletter = email.getMetadata("newsletter", Boolean.class);
        if (Boolean.TRUE.equals(isNewsletter)) {
            @SuppressWarnings("unchecked")
            var unsubscribeLinks = (java.util.Set<String>) email.getMetadata("unsubscribe_links");

            if (unsubscribeLinks != null && unsubscribeLinks.size() > 1) {
                log.info("🔗 Newsletter #{}: \"{}\" from {} - {} unsubscribe links found",
                        count,
                        truncateText(email.getSubject(), 50),
                        email.getSender(),
                        unsubscribeLinks.size());
            }
        }
    }

    private void generateReports() {
        log.info("📊 Generating newsletter analysis reports...");
        reportingService.generateNewsletterReport(newsletterAnalysisService);
    }

    private String truncateText(String text, int maxLength) {
        if (text == null) return "";
        return text.length() > maxLength ? text.substring(0, maxLength) + "..." : text;
    }
}