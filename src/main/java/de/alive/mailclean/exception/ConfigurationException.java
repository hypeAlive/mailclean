package de.alive.mailclean.exception;

public class ConfigurationException extends RuntimeException {

    private final String configKey;
    private final ConfigurationType type;

    public enum ConfigurationType {
        MISSING_ENVIRONMENT_VARIABLE,
        INVALID_VALUE,
        CONNECTION_SETTINGS,
        AUTHENTICATION_FAILED
    }

    public ConfigurationException(String message, String configKey, ConfigurationType type) {
        super(message);
        this.configKey = configKey;
        this.type = type;
    }

    public ConfigurationException(String message, String configKey, ConfigurationType type, Throwable cause) {
        super(message, cause);
        this.configKey = configKey;
        this.type = type;
    }

    public String getConfigKey() {
        return configKey;
    }

    public ConfigurationType getType() {
        return type;
    }

    public boolean isCritical() {
        return type == ConfigurationType.MISSING_ENVIRONMENT_VARIABLE ||
                type == ConfigurationType.AUTHENTICATION_FAILED;
    }
}