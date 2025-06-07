package de.alive.mailclean.service;

import de.alive.mailclean.domain.Email;
import de.alive.mailclean.exception.EmailProcessingException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Service interface for analyzing email content and metadata.
 * Implementations should be thread-safe and handle large volumes efficiently.
 */
public interface EmailAnalysisService {

    /**
     * Analyzes a single email and adds metadata based on the analysis.
     * This method should be thread-safe and handle exceptions gracefully.
     *
     * @param email the email to analyze (will be modified with metadata)
     * @throws EmailProcessingException if analysis fails critically
     */
    void analyzeEmail(Email email) throws EmailProcessingException;

    /**
     * Analyzes multiple emails in batch for better performance.
     * Default implementation processes emails individually.
     *
     * @param emails list of emails to analyze
     * @return analysis results with any errors encountered
     */
    default BatchAnalysisResult analyzeBatch(List<Email> emails) {
        BatchAnalysisResult result = new BatchAnalysisResult();

        for (Email email : emails) {
            try {
                analyzeEmail(email);
                result.incrementSuccessful();
            } catch (Exception e) {
                result.addError(email.getId(), e);
            }
        }

        return result;
    }

    /**
     * Gets analysis statistics for monitoring and reporting.
     *
     * @return current analysis statistics
     */
    AnalysisStatistics getStatistics();

    /**
     * Checks if the analyzer can handle the given email type efficiently.
     *
     * @param email the email to check
     * @return true if this analyzer should process this email
     */
    default boolean canAnalyze(Email email) {
        return true; // Default: can analyze any email
    }

    /**
     * Gets the priority of this analyzer (lower numbers = higher priority).
     *
     * @return priority value for ordering multiple analyzers
     */
    default int getPriority() {
        return 100; // Default medium priority
    }

    /**
     * Gets a description of what this analyzer does.
     *
     * @return human-readable description
     */
    String getDescription();

    /**
     * Resets internal state and statistics.
     */
    default void reset() {
        // Default: no-op
    }

    /**
     * Performs any necessary cleanup when shutting down.
     */
    default void shutdown() {
        // Default: no-op
    }

    /**
     * Result container for batch analysis operations.
     */
    class BatchAnalysisResult {
        private int successful = 0;
        private int failed = 0;
        private final java.util.Map<String, Exception> errors = new java.util.concurrent.ConcurrentHashMap<>();

        public void incrementSuccessful() {
            successful++;
        }

        public void addError(String emailId, Exception error) {
            failed++;
            errors.put(emailId, error);
        }

        public int getSuccessful() { return successful; }
        public int getFailed() { return failed; }
        public int getTotal() { return successful + failed; }
        public java.util.Map<String, Exception> getErrors() { return new java.util.HashMap<>(errors); }

        public double getSuccessRate() {
            int total = getTotal();
            return total > 0 ? (successful * 100.0) / total : 100.0;
        }

        @Override
        public String toString() {
            return String.format("BatchResult{successful=%d, failed=%d, rate=%.1f%%}",
                    successful, failed, getSuccessRate());
        }
    }

    /**
     * Statistics container for analysis operations.
     */
    interface AnalysisStatistics {

        long getTotalAnalyzed();
        long getTotalErrors();
        long getAnalysisTimeMs();

        default double getErrorRate() {
            long total = getTotalAnalyzed();
            return total > 0 ? (getTotalErrors() * 100.0) / total : 0.0;
        }

        default double getAverageAnalysisTimeMs() {
            long total = getTotalAnalyzed();
            return total > 0 ? (double) getAnalysisTimeMs() / total : 0.0;
        }

        default String getSummary() {
            return String.format("Analyzed: %,d | Errors: %,d (%.1f%%) | Avg Time: %.2fms",
                    getTotalAnalyzed(), getTotalErrors(), getErrorRate(), getAverageAnalysisTimeMs());
        }
    }
}