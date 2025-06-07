package de.alive.mailclean.newsletter;

import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class NewsletterReportingService {

    public void generateFinalReport(Set<String> unsubscribeLinks, Set<String> sendersWithLinks,
                                    int newsletterCount, int skippedCount) {
        logUnsubscribeLinks(unsubscribeLinks);
        logSenderCache(sendersWithLinks);
        logSummaryStatistics(newsletterCount, skippedCount, unsubscribeLinks.size());
    }

    private void logUnsubscribeLinks(Set<String> unsubscribeLinks) {
        if (!unsubscribeLinks.isEmpty()) {
            log.info("");
            log.info("{} UNSUBSCRIBE LINKS FOUND:", LogUtils.SEARCH_EMOJI);
            log.info("=============================================");

            List<String> sortedLinks = new ArrayList<>(unsubscribeLinks);
            sortedLinks.sort(String::compareTo);

            int linkNumber = 1;
            for (String link : sortedLinks) {
                log.info("   {}. {}", linkNumber++, link);
            }
            log.info("=============================================");
        }
    }

    private void logSenderCache(Set<String> sendersWithLinks) {
        if (!sendersWithLinks.isEmpty()) {
            log.info("");
            log.info("{} SENDERS WITH UNSUBSCRIBE LINKS:", LogUtils.EMAIL_EMOJI);
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

    private void logSummaryStatistics(int newsletterCount, int skippedCount, int linkCount) {
        log.info("");
        log.info("{} NEWSLETTER ANALYSIS SUMMARY:", LogUtils.CHART_EMOJI);
        log.info("=============================================");
        log.info("   {} Newsletter emails found: {}", LogUtils.EMAIL_EMOJI, newsletterCount);
        log.info("   {} Emails skipped (known senders): {}", LogUtils.SEARCH_EMOJI, skippedCount);
        log.info("   {} Unique unsubscribe links: {}", LogUtils.SEARCH_EMOJI, linkCount);
        log.info("=============================================");
    }
}