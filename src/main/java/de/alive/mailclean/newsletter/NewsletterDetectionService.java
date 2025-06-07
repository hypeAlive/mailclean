package de.alive.mailclean.newsletter;

import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class NewsletterDetectionService {

    // Enhanced keyword sets for better detection
    private static final Set<String> NEWSLETTER_KEYWORDS = Set.of(
            "newsletter", "unsubscribe", "abmelden", "abbestellen", "kündigen",
            "opt-out", "opt out", "remove", "stop", "delete", "cancel",
            "marketing", "werbung", "promotion", "angebot", "rabatt",
            "updates", "nachrichten", "news", "benachrichtigung",
            "digest", "bulletin", "weekly", "monthly", "daily",
            "notification", "alert", "campaign", "mailing"
    );

    private static final Set<String> NEWSLETTER_DOMAINS = Set.of(
            "mailchimp.com", "constantcontact.com", "sendgrid.net",
            "amazonses.com", "mailgun.org", "mandrill.com",
            "mailerlite.com", "convertkit.com", "aweber.com",
            "getresponse.com", "activecampaign.com", "drip.com",
            "campaign-archive.com", "us-east-1.amazonses.com",
            "sparkpostmail.com", "postmarkapp.com"
    );

    private static final Set<String> NEWSLETTER_HEADERS = Set.of(
            "list-unsubscribe", "precedence: bulk", "x-mailer",
            "x-campaign", "x-mailchimp", "x-sendgrid",
            "auto-submitted: auto-generated"
    );

    private static final Pattern UNSUBSCRIBE_PATTERN = Pattern.compile(
            "\\b(unsubscribe|abmelden|abbestellen|opt.?out|remove.?me|stop.?emails)\\b",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern BULK_SENDER_PATTERN = Pattern.compile(
            "\\b(no-?reply|noreply|donotreply|do-not-reply|newsletter|info|support|marketing)@",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern AUTOMATED_SUBJECT_PATTERN = Pattern.compile(
            "\\b(newsletter|digest|update|bulletin|news|notification|alert|weekly|monthly|daily)\\b",
            Pattern.CASE_INSENSITIVE
    );

    public boolean isNewsletterEmail(String subject, String sender, String content) {
        if (subject == null || sender == null || content == null) {
            log.debug("Null input detected - not a newsletter");
            return false;
        }

        try {
            // Multi-factor detection approach
            boolean result = checkSubjectPatterns(subject) ||
                    checkSenderPatterns(sender) ||
                    checkContentIndicators(content) ||
                    checkKeywordMatches(subject, sender, content);

            if (result) {
                log.debug("{} Newsletter detected: sender={}, subject={}",
                        LogUtils.EMAIL_EMOJI,
                        maskEmail(sender),
                        truncateText(subject, 50));
            }

            return result;

        } catch (Exception e) {
            log.debug("Error in newsletter detection for sender {}: {}",
                    maskEmail(sender), e.getMessage());
            return false;
        }
    }

    private boolean checkSubjectPatterns(String subject) {
        if (subject == null || subject.trim().isEmpty()) {
            return false;
        }

        String normalizedSubject = subject.toLowerCase().trim();

        // Check for automated subject patterns
        if (AUTOMATED_SUBJECT_PATTERN.matcher(normalizedSubject).find()) {
            log.debug("Newsletter detected by subject pattern: {}", truncateText(subject, 50));
            return true;
        }

        // Check for numbered/dated newsletters
        if (normalizedSubject.matches(".*\\b(#\\d+|vol\\.?\\s*\\d+|issue\\s*\\d+|\\d{1,2}[./]\\d{1,2}[./]\\d{2,4}).*")) {
            log.debug("Newsletter detected by numbering/dating in subject: {}", truncateText(subject, 50));
            return true;
        }

        return false;
    }

    private boolean checkSenderPatterns(String sender) {
        if (sender == null || sender.trim().isEmpty()) {
            return false;
        }

        String normalizedSender = sender.toLowerCase().trim();

        // Check for bulk sender patterns
        if (BULK_SENDER_PATTERN.matcher(normalizedSender).find()) {
            log.debug("Newsletter detected by sender pattern: {}", maskEmail(sender));
            return true;
        }

        // Check for known newsletter domains
        for (String domain : NEWSLETTER_DOMAINS) {
            if (normalizedSender.contains(domain)) {
                log.debug("Newsletter detected by domain: {}", domain);
                return true;
            }
        }

        return false;
    }

    private boolean checkContentIndicators(String content) {
        if (content == null || content.trim().isEmpty()) {
            return false;
        }

        String normalizedContent = content.toLowerCase();

        // Check for unsubscribe patterns in content
        if (UNSUBSCRIBE_PATTERN.matcher(normalizedContent).find()) {
            log.debug("Newsletter detected by unsubscribe pattern in content");
            return true;
        }

        // Check for common newsletter footer patterns
        if (normalizedContent.contains("you received this email") ||
                normalizedContent.contains("manage your preferences") ||
                normalizedContent.contains("update your email preferences") ||
                normalizedContent.contains("click here to unsubscribe") ||
                normalizedContent.contains("view this email in your browser")) {
            log.debug("Newsletter detected by footer patterns");
            return true;
        }

        // Check for tracking pixels and analytics
        if (normalizedContent.contains("?utm_") ||
                normalizedContent.contains("google-analytics") ||
                normalizedContent.contains("tracking") ||
                normalizedContent.matches(".*\\b[a-f0-9]{32}\\b.*")) { // MD5 tracking IDs
            log.debug("Newsletter detected by tracking indicators");
            return true;
        }

        return false;
    }

    private boolean checkKeywordMatches(String subject, String sender, String content) {
        // Combine all text for comprehensive keyword search
        String searchText = String.join(" ",
                subject != null ? subject : "",
                sender != null ? sender : "",
                content != null ? content.substring(0, Math.min(content.length(), 1000)) : ""
        ).toLowerCase();

        // Check for newsletter keywords with context
        for (String keyword : NEWSLETTER_KEYWORDS) {
            if (searchText.contains(keyword)) {
                // Additional context check to reduce false positives
                if (isKeywordInNewsletterContext(searchText, keyword)) {
                    log.debug("Newsletter detected by keyword '{}' in proper context", keyword);
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isKeywordInNewsletterContext(String text, String keyword) {
        // Simple context analysis - keyword should appear near other newsletter indicators
        String[] contextWords = {"email", "mail", "list", "subscription", "update", "news", "weekly", "monthly"};

        int keywordIndex = text.indexOf(keyword);
        if (keywordIndex == -1) return false;

        // Check 100 characters before and after the keyword
        int start = Math.max(0, keywordIndex - 100);
        int end = Math.min(text.length(), keywordIndex + keyword.length() + 100);
        String context = text.substring(start, end);

        for (String contextWord : contextWords) {
            if (context.contains(contextWord)) {
                return true;
            }
        }

        return false;
    }

    public double calculateNewsletterConfidence(String subject, String sender, String content) {
        double confidence = 0.0;
        int factors = 0;

        if (checkSubjectPatterns(subject)) {
            confidence += 0.3;
            factors++;
        }

        if (checkSenderPatterns(sender)) {
            confidence += 0.4;
            factors++;
        }

        if (checkContentIndicators(content)) {
            confidence += 0.5;
            factors++;
        }

        if (checkKeywordMatches(subject, sender, content)) {
            confidence += 0.2;
            factors++;
        }

        // Normalize confidence based on number of factors
        return factors > 0 ? Math.min(1.0, confidence) : 0.0;
    }

    public NewsletterAnalysisResult analyzeEmail(String subject, String sender, String content) {
        boolean isNewsletter = isNewsletterEmail(subject, sender, content);
        double confidence = calculateNewsletterConfidence(subject, sender, content);

        return new NewsletterAnalysisResult(
                isNewsletter,
                confidence,
                determineDetectionReason(subject, sender, content)
        );
    }

    private String determineDetectionReason(String subject, String sender, String content) {
        if (checkSubjectPatterns(subject)) {
            return "Subject pattern analysis";
        }
        if (checkSenderPatterns(sender)) {
            return "Sender pattern analysis";
        }
        if (checkContentIndicators(content)) {
            return "Content indicators";
        }
        if (checkKeywordMatches(subject, sender, content)) {
            return "Keyword matching";
        }
        return "No newsletter indicators found";
    }

    // Utility methods
    private String maskEmail(String email) {
        if (email == null || !email.contains("@")) return "***";
        String[] parts = email.split("@");
        return parts[0].substring(0, Math.min(2, parts[0].length())) + "***@" + parts[1];
    }

    private String truncateText(String text, int maxLength) {
        if (text == null) return "";
        return text.length() > maxLength ? text.substring(0, maxLength) + "..." : text;
    }

    // Result class for detailed analysis
    public record NewsletterAnalysisResult(
            boolean isNewsletter,
            double confidence,
            String detectionReason
    ) {
        @Override
        public String toString() {
            return String.format("Newsletter: %s (%.1f%% confidence) - %s",
                    isNewsletter ? "YES" : "NO",
                    confidence * 100,
                    detectionReason);
        }
    }
}