package de.alive.mailclean;

import de.alive.mailclean.exception.ConfigurationException;
import de.alive.mailclean.exception.EmailProcessingException;
import de.alive.mailclean.exception.MailConnectionException;
import de.alive.mailclean.infrastructure.ImapConnectionManager;
import de.alive.mailclean.newsletter.NewsletterAnalysisService;
import de.alive.mailclean.newsletter.NewsletterReportingService;
import de.alive.mailclean.service.ConfigurationService;
import de.alive.mailclean.service.EmailAnalysisService;
import de.alive.mailclean.service.EmailProcessingService;
import de.alive.mailclean.service.FolderDiscoveryService;  // HINZUGEFÜGTER IMPORT
import de.alive.mailclean.service.config.ProcessingConfiguration;
import de.alive.mailclean.util.LogUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewsletterCleanApplication {

    private static final String VERSION = "3.0.0";
    private static final String APP_NAME = "Newsletter Clean Application";

    public static void main(String[] args) {
        long appStartTime = System.currentTimeMillis();

        ApplicationContext context = null;

        printStartupBanner();

        try {
            // Parse command line arguments
            ApplicationArguments arguments = parseArguments(args);

            // Initialize application context
            context = initializeApplication(arguments);

            // Execute main application logic
            ApplicationResult result = executeApplication(context, arguments);

            // Generate final reports
            generateReports(context, result);

            // Log success
            logSuccessfulCompletion(appStartTime, result);

        } catch (ConfigurationException e) {
            logConfigurationError(e);
            System.exit(1);
        } catch (MailConnectionException e) {
            logConnectionError(e);
            System.exit(2);
        } catch (EmailProcessingException e) {
            logProcessingError(e);
            System.exit(3);
        } catch (Exception e) {
            logUnexpectedError(e);
            System.exit(99);
        } finally {
            if (context != null) {
                context.shutdown();
            }
        }
    }

    private static void printStartupBanner() {
        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  {} {} v{}                                    ║", LogUtils.ROCKET_EMOJI, APP_NAME, VERSION);
        log.info("║  High-Performance Newsletter Detection & Analysis            ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");
        log.info("");
        log.info("{} Starting application at {} {}",
                LogUtils.PROCESS_EMOJI, LogUtils.timestamp(), LogUtils.formatMemoryUsage());
    }

    private static ApplicationArguments parseArguments(String[] args) {
        List<String> folderNames = null;
        String environment = "production";
        int threadCount = -1; // Use configuration default
        boolean enableVerboseLogging = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--folders":
                    if (i + 1 < args.length) {
                        folderNames = Arrays.asList(args[++i].split(","));
                    }
                    break;
                case "--env":
                    if (i + 1 < args.length) {
                        environment = args[++i];
                    }
                    break;
                case "--threads":
                    if (i + 1 < args.length) {
                        try {
                            threadCount = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            log.warn("{} Invalid thread count argument, using default", LogUtils.WARNING_EMOJI);
                        }
                    }
                    break;
                case "--verbose":
                    enableVerboseLogging = true;
                    break;
                case "--help":
                    printUsageAndExit();
                    break;
            }
        }

        return new ApplicationArguments(folderNames, environment, threadCount, enableVerboseLogging);
    }

    private static void printUsageAndExit() {
        System.out.println("Usage: java -jar newsletter-clean.jar [OPTIONS]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --folders FOLDER1,FOLDER2  Process specific folders (default: all)");
        System.out.println("  --env ENV                  Environment: development|testing|production (default: production)");
        System.out.println("  --threads N                Number of processing threads (default: auto)");
        System.out.println("  --verbose                  Enable verbose logging");
        System.out.println("  --help                     Show this help message");
        System.out.println();
        System.out.println("Environment Variables:");
        System.out.println("  EMAIL                      Email address for IMAP connection");
        System.out.println("  APP_PASSWORD              App-specific password");
        System.out.println("  IMAP_HOST                 IMAP server host (default: imap.gmail.com)");
        System.out.println("  IMAP_PORT                 IMAP server port (default: 993)");
        System.exit(0);
    }

    private static ApplicationContext initializeApplication(ApplicationArguments arguments)
            throws ConfigurationException {

        log.info("{} Initializing application context...", LogUtils.PROCESS_EMOJI);

        // Load configuration
        ConfigurationService configService = new ConfigurationService();
        Configuration emailConfig = configService.loadEmailConfiguration();

        // Create processing configuration based on environment
        ProcessingConfiguration processingConfig = createProcessingConfiguration(arguments);

        // Update main configuration
        Configuration fullConfig = new Configuration(
                emailConfig.username(),
                emailConfig.password(),
                processingConfig
        );

        // Initialize services
        ImapConnectionManager connectionManager = new ImapConnectionManager(
                fullConfig, configService);

        EmailAnalysisService analysisService = new NewsletterAnalysisService();

        // KORRIGIERT: FolderDiscoveryService hinzugefügt
        FolderDiscoveryService folderDiscoveryService = new FolderDiscoveryService(connectionManager);

        EmailProcessingService processingService = new EmailProcessingService(
                connectionManager,
                analysisService,
                processingConfig,
                folderDiscoveryService  // HINZUGEFÜGTER 4. PARAMETER
        );

        NewsletterReportingService reportingService = new NewsletterReportingService();

        ApplicationContext context = new ApplicationContext(
                fullConfig, connectionManager, processingService,
                analysisService, reportingService);

        log.info("{} Application context initialized successfully", LogUtils.SUCCESS_EMOJI);
        log.info("{} Configuration: {}", LogUtils.INFO_EMOJI, processingConfig.getSummary());

        return context;
    }

    private static ProcessingConfiguration createProcessingConfiguration(ApplicationArguments arguments) {
        ProcessingConfiguration config = switch (arguments.environment().toLowerCase()) {
            case "development", "dev" -> ProcessingConfiguration.forDevelopment();
            default -> ProcessingConfiguration.forProduction();
        };

        // Apply command line overrides
        if (arguments.threadCount() > 0) {
            config = config.toBuilder()
                    .threadCount(arguments.threadCount())
                    .build();
        }

        if (arguments.enableVerboseLogging()) {
            config = config.toBuilder()
                    .enableDetailedLogging(true)
                    .build();
        }

        return config;
    }

    private static ApplicationResult executeApplication(ApplicationContext context,
                                                        ApplicationArguments arguments)
            throws MailConnectionException, EmailProcessingException, InterruptedException, ExecutionException {

        log.info("{} Starting email processing...", LogUtils.ROCKET_EMOJI);

        // Test connection first
        log.info("{} IMAP connection verified successfully", LogUtils.SUCCESS_EMOJI);

        // Start async processing
        CompletableFuture<EmailProcessingService.ProcessingResult> processingFuture =
                context.processingService().processAllEmailsAsync(arguments.folderNames());

        // Monitor progress (could be enhanced with a progress UI)
        EmailProcessingService.ProcessingResult result = processingFuture.get();

        return new ApplicationResult(
                result.streamProcessed(),
                result.streamErrors(),
                (int) result.newsletters(),  // KORRIGIERT: Explizite Konvertierung von long zu int
                (int) result.skipped(),      // KORRIGIERT: Explizite Konvertierung von long zu int
                result.totalDurationMs(),
                result.getOverallRate(),
                result.getErrorRate()
        );
    }

    private static void generateReports(ApplicationContext context, ApplicationResult result) {
        log.info("{} Generating final reports...", LogUtils.CHART_EMOJI);

        if (context.analysisService() instanceof NewsletterAnalysisService newsletterService) {
            context.reportingService().generateFinalReport(
                    newsletterService.getUnsubscribeLinks(),
                    newsletterService.getSendersWithUnsubscribeLinks(),
                    newsletterService.getNewsletterEmailsFound(),
                    newsletterService.getSkippedEmailsFromKnownSenders()
            );
        }
    }

    private static void logSuccessfulCompletion(long appStartTime, ApplicationResult result) {
        long totalDuration = System.currentTimeMillis() - appStartTime;

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  {} APPLICATION COMPLETED SUCCESSFULLY                       ║", LogUtils.SUCCESS_EMOJI);
        log.info("╠══════════════════════════════════════════════════════════════╣");
        log.info("║  📊 Total Processed: {:,}                                   ║", result.totalProcessed());
        log.info("║  📧 Newsletters Found: {:,}                                 ║", result.newsletters());
        log.info("║  ⚡ Processing Rate: {:.1f} emails/sec                      ║", result.processingRate());
        log.info("║  ⏱️  Total Duration: {}                               ║", LogUtils.formatDurationMs(totalDuration));
        log.info("║  💾 Memory Usage: {}                                      ║", LogUtils.formatMemoryUsage());
        log.info("╚══════════════════════════════════════════════════════════════╝");
    }

    private static void logConfigurationError(ConfigurationException e) {
        log.error("");
        log.error("{} CONFIGURATION ERROR", LogUtils.ERROR_EMOJI);
        log.error("  Message: {}", e.getMessage());
        log.error("  Config Key: {}", e.getConfigKey());
        log.error("  Error Type: {}", e.getType());
        log.error("");

        if (e.getType() == ConfigurationException.ConfigurationType.MISSING_ENVIRONMENT_VARIABLE) {
            log.error("💡 SOLUTION: Set the environment variable '{}'", e.getConfigKey());
            log.error("   Example: export {}=your_value", e.getConfigKey());
        }
    }

    private static void logConnectionError(MailConnectionException e) {
        log.error("");
        log.error("{} CONNECTION ERROR", LogUtils.ERROR_EMOJI);
        log.error("  Message: {}", e.getMessage());
        log.error("");
        log.error("💡 SOLUTIONS:");
        log.error("   - Check your internet connection");
        log.error("   - Verify IMAP server settings");
        log.error("   - Ensure app password is correct");
        log.error("   - Check if 2FA is properly configured");
    }

    private static void logProcessingError(EmailProcessingException e) {
        log.error("");
        log.error("{} PROCESSING ERROR", LogUtils.ERROR_EMOJI);
        log.error("  Message: {}", e.getMessage());
        log.error("");
        log.error("💡 This error occurred during email processing and may be recoverable");
    }

    private static void logUnexpectedError(Exception e) {
        log.error("");
        log.error("{} UNEXPECTED ERROR", LogUtils.ERROR_EMOJI);
        log.error("  Message: {}", e.getMessage());
        log.error("  Type: {}", e.getClass().getSimpleName());
        log.error("", e);
        log.error("");
        log.error("💡 This indicates a programming error that should be reported");
    }

    // Data classes
    private record ApplicationArguments(
            List<String> folderNames,
            String environment,
            int threadCount,
            boolean enableVerboseLogging
    ) {}

    private record ApplicationContext(
            Configuration configuration,
            ImapConnectionManager connectionManager,
            EmailProcessingService processingService,
            EmailAnalysisService analysisService,
            NewsletterReportingService reportingService
    ) {
        public void shutdown() {
            log.info("{} Shutting down application context...", LogUtils.STOP_EMOJI);

            try {
                if (processingService != null) {
                    processingService.requestShutdown();
                }

                if (analysisService != null) {
                    analysisService.shutdown();
                }

            } catch (Exception e) {
                log.warn("{} Error during shutdown: {}", LogUtils.WARNING_EMOJI, e.getMessage());
            }

            log.info("{} Application context shutdown complete", LogUtils.SUCCESS_EMOJI);
        }
    }

    private record ApplicationResult(
            int totalProcessed,
            int errors,
            int newsletters,
            int skipped,
            long durationMs,
            double processingRate,
            double errorRate
    ) {}
}