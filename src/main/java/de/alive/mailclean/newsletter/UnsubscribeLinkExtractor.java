package de.alive.mailclean.newsletter;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class UnsubscribeLinkExtractor {

    private static final List<Pattern> UNSUBSCRIBE_PATTERNS = List.of(
            Pattern.compile("https?://[^\\s\"'<>]+unsubscribe[^\\s\"'<>]*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s\"'<>]+opt.?out[^\\s\"'<>]*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("https?://[^\\s\"'<>]+abmelden[^\\s\"'<>]*", Pattern.CASE_INSENSITIVE),
            Pattern.compile("<a[^>]+href=[\"']([^\"']*unsubscribe[^\"']*)[\"'][^>]*>", Pattern.CASE_INSENSITIVE),
            Pattern.compile("<a[^>]+href=[\"']([^\"']*opt.?out[^\"']*)[\"'][^>]*>", Pattern.CASE_INSENSITIVE)
    );

    public Set<String> extractUnsubscribeLinks(String content) {
        Set<String> links = new HashSet<>();

        if (content == null || content.trim().isEmpty()) {
            return links;
        }

        for (Pattern pattern : UNSUBSCRIBE_PATTERNS) {
            Matcher matcher = pattern.matcher(content);
            while (matcher.find()) {
                String link = matcher.groupCount() > 0 ? matcher.group(1) : matcher.group();
                link = cleanLink(link);

                if (isValidLink(link)) {
                    links.add(link);
                }
            }
        }

        return links;
    }

    private String cleanLink(String link) {
        if (link == null) return "";

        return link.trim()
                .replaceAll("&amp;", "&")
                .replaceAll("&lt;", "<")
                .replaceAll("&gt;", ">")
                .replaceAll("&quot;", "\"");
    }

    private boolean isValidLink(String link) {
        if (link == null || link.length() < 10) {
            return false;
        }

        try {
            new URL(link);
            return link.startsWith("http://") || link.startsWith("https://");
        } catch (Exception e) {
            return false;
        }
    }
}