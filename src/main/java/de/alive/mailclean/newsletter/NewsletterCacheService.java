package de.alive.mailclean.newsletter;

import lombok.extern.slf4j.Slf4j;

import javax.mail.internet.InternetAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class NewsletterCacheService {

    private final Set<String> sendersWithUnsubscribeLinks = ConcurrentHashMap.newKeySet();
    private final Set<String> unsubscribeLinks = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Object> senderProcessingLocks = new ConcurrentHashMap<>();

    private final AtomicInteger newsletterEmailsFound = new AtomicInteger(0);
    private final AtomicInteger skippedEmailsFromKnownSenders = new AtomicInteger(0);
    private final AtomicInteger newSendersDiscovered = new AtomicInteger(0);

    public boolean isSenderKnown(String normalizedSender) {
        return sendersWithUnsubscribeLinks.contains(normalizedSender);
    }

    public void addSenderToCache(String normalizedSender) {
        sendersWithUnsubscribeLinks.add(normalizedSender);
        log.debug("Sender '{}' added to cache", normalizedSender);
    }

    public void addUnsubscribeLinks(Set<String> links) {
        unsubscribeLinks.addAll(links);
    }

    public Object getSenderLock(String normalizedSender) {
        return senderProcessingLocks.computeIfAbsent(normalizedSender, k -> new Object());
    }

    public void removeSenderLock(String normalizedSender) {
        senderProcessingLocks.remove(normalizedSender);
    }

    public Set<String> getCachedSenders() {
        return Set.copyOf(sendersWithUnsubscribeLinks);
    }

    public Set<String> getAllUnsubscribeLinks() {
        return Set.copyOf(unsubscribeLinks);
    }

    public int getCacheSize() {
        return sendersWithUnsubscribeLinks.size();
    }

    public String normalizeSenderAddress(String from) {
        if (from == null || from.trim().isEmpty()) {
            return "";
        }

        try {
            InternetAddress[] addresses = InternetAddress.parse(from);
            if (addresses.length > 0) {
                String email = addresses[0].getAddress();
                if (email != null) {
                    return email.toLowerCase().trim();
                }
            }
        } catch (Exception e) {
            Pattern emailPattern = Pattern.compile("([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})");
            Matcher matcher = emailPattern.matcher(from);
            if (matcher.find()) {
                return matcher.group(1).toLowerCase().trim();
            }
        }

        return from.toLowerCase().trim();
    }

    public void incrementNewsletterEmails() { newsletterEmailsFound.incrementAndGet(); }
    public void incrementSkippedEmails() { skippedEmailsFromKnownSenders.incrementAndGet(); }
    public void incrementNewSenders() { newSendersDiscovered.incrementAndGet(); }

    public int getNewsletterEmailsFound() { return newsletterEmailsFound.get(); }
    public int getSkippedEmailsFromKnownSenders() { return skippedEmailsFromKnownSenders.get(); }
    public int getNewSendersDiscovered() { return newSendersDiscovered.get(); }
}