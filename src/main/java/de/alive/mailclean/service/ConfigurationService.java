package de.alive.mailclean.service;

import de.alive.mailclean.Configuration;
import de.alive.mailclean.exception.ConfigurationException;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Properties;

@Slf4j
public class ConfigurationService {

    private static final String EMAIL_ENV = "EMAIL";
    private static final String PASSWORD_ENV = "APP_PASSWORD";
    private static final String IMAP_HOST_ENV = "IMAP_HOST";
    private static final String IMAP_PORT_ENV = "IMAP_PORT";

    private static final String DEFAULT_IMAP_HOST = "imap.gmail.com";
    private static final String DEFAULT_IMAP_PORT = "993";

    public Configuration loadEmailConfiguration() {
        log.info("{} Loading email configuration...", LogUtils.PROCESS_EMOJI);

        String email = getRequiredEnvironmentVariable(EMAIL_ENV);
        String password = getRequiredEnvironmentVariable(PASSWORD_ENV);

        validateEmailFormat(email);
        validatePasswordSecurity(password);

        log.info("{} Email configuration loaded successfully for: {}",
                LogUtils.SUCCESS_EMOJI, maskEmail(email));

        return new Configuration(email, password);
    }

    public Properties createImapProperties() {
        String host = getEnvironmentVariable(IMAP_HOST_ENV).orElse(DEFAULT_IMAP_HOST);
        String port = getEnvironmentVariable(IMAP_PORT_ENV).orElse(DEFAULT_IMAP_PORT);

        log.info("{} Configuring IMAP connection: {}:{}", LogUtils.PROCESS_EMOJI, host, port);

        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imaps");
        props.setProperty("mail.imaps.host", host);
        props.setProperty("mail.imaps.port", port);
        props.setProperty("mail.imaps.ssl.enable", "true");
        props.setProperty("mail.imaps.ssl.trust", "*");
        props.setProperty("mail.imaps.connectionpoolsize", "20");
        props.setProperty("mail.imaps.connectionpooltimeout", "300000");
        props.setProperty("mail.imaps.connectiontimeout", "10000");
        props.setProperty("mail.imaps.timeout", "10000");
        props.setProperty("mail.imaps.peek", "true");
        props.setProperty("mail.imaps.fetchsize", "16384");

        return props;
    }

    private String getRequiredEnvironmentVariable(String key) {
        return getEnvironmentVariable(key)
                .orElseThrow(() -> new ConfigurationException(
                        String.format("Required environment variable '%s' is not set", key),
                        key,
                        ConfigurationException.ConfigurationType.MISSING_ENVIRONMENT_VARIABLE
                ));
    }

    private Optional<String> getEnvironmentVariable(String key) {
        String value = System.getenv(key);
        return Optional.ofNullable(value)
                .filter(v -> !v.trim().isEmpty());
    }

    private void validateEmailFormat(String email) {
        if (!email.contains("@") || !email.contains(".")) {
            throw new ConfigurationException(
                    String.format("Invalid email format: %s", maskEmail(email)),
                    EMAIL_ENV,
                    ConfigurationException.ConfigurationType.INVALID_VALUE
            );
        }
    }

    private void validatePasswordSecurity(String password) {
        if (password.length() < 16) {
            log.warn("{} App password seems short - ensure it's a valid app-specific password",
                    LogUtils.WARNING_EMOJI);
        }
    }

    private String maskEmail(String email) {
        if (email == null || !email.contains("@")) return "***";

        String[] parts = email.split("@");
        String username = parts[0];
        String domain = parts[1];

        if (username.length() <= 2) {
            return "***@" + domain;
        }

        return username.substring(0, 2) + "***@" + domain;
    }
}