package de.alive.mailclean.newsletter;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class UnsubscribeLinkExtractor {

    private static final Pattern[] UNSUBSCRIBE_PATTERNS = {
            Pattern.compile("https?://[^\\s<>\"']+unsubscribe[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+opt[_-]?out[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+remove[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+abmeld[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+abbestel[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+cancel[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']+stop[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("<?https?://[^\\s<>\"']+(?:unsubscribe|opt[_-]?out|remove|abmeld|abbestel)[^\\s<>\"']*>?", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']*mailchimp[^\\s<>\"']*unsubscribe[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']*constantcontact[^\\s<>\"']*unsubscribe[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']*sendgrid[^\\s<>\"']*unsubscribe[^\\s<>\"']*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s<>\"']*\\.(?:us\\d+\\.list-manage\\.com|constantcontact\\.com|sendgrid\\.net|mailgun\\.org)[^\\s<>\"']*unsubscribe[^\\s<>\"']*", Pattern.CASE_INSENSITIVE)
    };

    public Set<String> extractUnsubscribeLinks(String content) {
        Set<String> links = new HashSet<>();

        if (content == null || content.trim().isEmpty()) {
            return links;
        }

        for (Pattern pattern : UNSUBSCRIBE_PATTERNS) {
            Matcher matcher = pattern.matcher(content);
            while (matcher.find()) {
                String link = matcher.group().trim();

                link = cleanLink(link);

                if (isValidLink(link)) {
                    links.add(link);
                }
            }
        }

        return links;
    }

    private String cleanLink(String link) {
        if (link.startsWith("<") && link.endsWith(">")) {
            link = link.substring(1, link.length() - 1);
        }

        link = link.replaceAll("[.,;!?]+$", "");

        return link.trim();
    }

    private boolean isValidLink(String link) {
        return link.length() > 10 &&
                (link.startsWith("http://") || link.startsWith("https://")) &&
                link.contains(".");
    }
}