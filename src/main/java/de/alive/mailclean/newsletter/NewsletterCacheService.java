package de.alive.mailclean.newsletter;

import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class NewsletterCacheService {

    private final Set<String> cachedSenders = ConcurrentHashMap.newKeySet();
    private final Set<String> unsubscribeLinks = ConcurrentHashMap.newKeySet();
    private final Map<String, Object> senderLocks = new ConcurrentHashMap<>();
    private final ReentrantLock lockMapLock = new ReentrantLock();

    private final AtomicInteger newsletterEmails = new AtomicInteger(0);
    private final AtomicInteger skippedEmails = new AtomicInteger(0);

    public String normalizeSenderAddress(String sender) {
        if (sender == null) return "";

        return sender.toLowerCase()
                .replaceAll("\\s+", " ")
                .trim();
    }

    public boolean isSenderKnown(String normalizedSender) {
        return cachedSenders.contains(normalizedSender);
    }

    public void addSenderToCache(String normalizedSender) {
        cachedSenders.add(normalizedSender);
        log.debug("{} Added sender to cache: {}", LogUtils.SEARCH_EMOJI, normalizedSender);
    }

    public void addUnsubscribeLinks(Set<String> links) {
        unsubscribeLinks.addAll(links);
    }

    public Object getSenderLock(String normalizedSender) {
        lockMapLock.lock();
        try {
            return senderLocks.computeIfAbsent(normalizedSender, k -> new Object());
        } finally {
            lockMapLock.unlock();
        }
    }

    public void removeSenderLock(String normalizedSender) {
        lockMapLock.lock();
        try {
            senderLocks.remove(normalizedSender);
        } finally {
            lockMapLock.unlock();
        }
    }

    public void incrementNewsletterEmails() {
        newsletterEmails.incrementAndGet();
    }

    public void incrementSkippedEmails() {
        skippedEmails.incrementAndGet();
    }

    public Set<String> getCachedSenders() {
        return Set.copyOf(cachedSenders);
    }

    public Set<String> getAllUnsubscribeLinks() {
        return Set.copyOf(unsubscribeLinks);
    }

    public int getNewsletterEmailsFound() {
        return newsletterEmails.get();
    }

    public int getSkippedEmailsFromKnownSenders() {
        return skippedEmails.get();
    }

    public void clearCache() {
        cachedSenders.clear();
        unsubscribeLinks.clear();
        senderLocks.clear();
        newsletterEmails.set(0);
        skippedEmails.set(0);
        log.info("{} Newsletter cache cleared", LogUtils.SEARCH_EMOJI);
    }
}