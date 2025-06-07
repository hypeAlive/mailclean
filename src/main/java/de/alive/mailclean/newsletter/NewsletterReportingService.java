package de.alive.mailclean.newsletter;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class NewsletterReportingService {

    public void generateNewsletterReport(NewsletterAnalysisService analysisService) {
        var unsubscribeLinks = analysisService.getUnsubscribeLinks();
        var sendersWithLinks = analysisService.getSendersWithUnsubscribeLinks();
        int totalNewsletters = analysisService.getNewsletterEmailsFound();
        int totalSkipped = analysisService.getSkippedEmailsFromKnownSenders();
        int newSenders = analysisService.getNewSendersDiscovered();

        logNewsletterStatistics(totalNewsletters, unsubscribeLinks.size(), sendersWithLinks.size(), totalSkipped, newSenders);
        logSenderCache(sendersWithLinks);
        logUnsubscribeLinks(unsubscribeLinks);
        logDomainAnalysis(unsubscribeLinks);
        logProviderAnalysis(unsubscribeLinks);
    }

    private void logNewsletterStatistics(int newsletters, int links, int senders, int skipped, int newSenders) {
        log.info("\n🎯 ================ NEWSLETTER ANALYSIS REPORT ================");
        log.info("📧 Total newsletters found: {}", newsletters);
        log.info("🔗 Unique unsubscribe links found: {}", links);
        log.info("👤 Senders with unsubscribe links (cache): {}", senders);
        log.info("⚡ Skipped emails (cache hits): {}", skipped);
        log.info("🆕 New senders discovered: {}", newSenders);

        if (skipped > 0) {
            double efficiency = (skipped * 100.0) / (newsletters + skipped);
            log.info("📈 Cache efficiency: {:.1f}% of newsletter emails skipped", efficiency);
        }

        if (newsletters > 0) {
            double linksPerNewsletter = (double) links / newsletters;
            log.info("📊 Average: {:.2f} unique links per newsletter", linksPerNewsletter);
        }
    }

    private void logSenderCache(Set<String> sendersWithLinks) {
        if (!sendersWithLinks.isEmpty()) {
            log.info("");
            log.info("👤 SENDERS WITH UNSUBSCRIBE LINKS (CACHE):");
            log.info("=============================================");

            List<String> sortedSenders = new ArrayList<>(sendersWithLinks);
            sortedSenders.sort(String::compareTo);

            int senderNumber = 1;
            for (String sender : sortedSenders) {
                log.info("   {}. {}", senderNumber++, sender);
            }
            log.info("=============================================");
        }
    }

    private void logUnsubscribeLinks(Set<String> unsubscribeLinks) {
        if (!unsubscribeLinks.isEmpty()) {
            log.info("");
            log.info("🔗 ALL FOUND UNSUBSCRIBE LINKS:");
            log.info("=======================================================");

            List<String> sortedLinks = new ArrayList<>(unsubscribeLinks);
            sortedLinks.sort(String::compareTo);

            int linkNumber = 1;
            for (String link : sortedLinks) {
                log.info("   {}. {}", linkNumber++, link);
            }
            log.info("=======================================================");
        } else {
            log.info("❌ No unsubscribe links found!");
            log.info("💡 Possible reasons:");
            log.info("   - No newsletters in the emails");
            log.info("   - Links are hidden in images or JavaScript");
            log.info("   - Unusual link formats");
        }
    }

    private void logDomainAnalysis(Set<String> links) {
        log.info("");
        log.info("🏷️ TOP DOMAINS:");

        Map<String, Integer> domainCount = links.stream()
                .map(this::extractDomain)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(
                        domain -> domain,
                        Collectors.collectingAndThen(Collectors.counting(), Math::toIntExact)
                ));

        domainCount.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry ->
                        log.info("   {} Links - {}", entry.getValue(), entry.getKey())
                );
    }

    private void logProviderAnalysis(Set<String> links) {
        log.info("");
        log.info("📮 NEWSLETTER PROVIDERS:");

        String[] knownProviders = {
                "mailchimp", "constantcontact", "sendgrid", "mailgun",
                "aweber", "getresponse", "convertkit", "activecampaign",
                "klaviyo", "omnisend", "sendinblue", "mailerlite"
        };

        for (String provider : knownProviders) {
            long count = links.stream()
                    .filter(link -> link.toLowerCase().contains(provider))
                    .count();
            if (count > 0) {
                log.info("   {} Links - {}", count, provider.toUpperCase());
            }
        }

        log.info("🎯 ================================================================");
    }

    private String extractDomain(String link) {
        try {
            URL url = new URL(link);
            return url.getHost().toLowerCase();
        } catch (Exception e) {
            return null;
        }
    }
}