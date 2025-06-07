package de.alive.mailclean.newsletter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NewsletterDetectionService {

    private static final String[] NEWSLETTER_KEYWORDS = {
            "newsletter", "unsubscribe", "abmelden", "abbestellen", "kündigen",
            "opt-out", "remove", "stop", "delete", "cancel",
            "marketing", "werbung", "promotion", "angebot", "rabatt",
            "updates", "nachrichten", "news", "benachrichtigung"
    };

    public boolean isNewsletterEmail(String subject, String from, String content) {
        try {
            // Quick keyword check
            String searchText = (subject + " " + from + " " + content).toLowerCase();

            for (String keyword : NEWSLETTER_KEYWORDS) {
                if (searchText.contains(keyword)) {
                    return true;
                }
            }

            return false;

        } catch (Exception e) {
            log.debug("Error in newsletter detection: {}", e.getMessage());
            return false;
        }
    }
}