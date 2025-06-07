package de.alive.mailclean;

import de.alive.mailclean.service.config.ProcessingConfiguration;

/**
 * Main configuration record for email credentials and processing settings.
 */
public record Configuration(
        String username,
        String password,
        ProcessingConfiguration processingConfig
) {

    public Configuration(String username, String password) {
        this(username, password, ProcessingConfiguration.forProduction());
    }

    public Configuration {
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        if (password == null || password.trim().isEmpty()) {
            throw new IllegalArgumentException("Password cannot be null or empty");
        }
        if (processingConfig == null) {
            throw new IllegalArgumentException("Processing configuration cannot be null");
        }
    }
}