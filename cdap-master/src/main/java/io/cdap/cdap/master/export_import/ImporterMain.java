package io.cdap.cdap.master.export_import;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.SupplierProviderBridge;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.guice.RemoteLogAppenderModule;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImporterMain {
  private static final Logger LOG = LoggerFactory.getLogger(ImporterMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_RETRY_DELAY_MS = 1000L;

  public static void main(String[] args) throws IOException {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
        Constants.Logging.COMPONENT_NAME,
        Constants.Service.IMPORTER));
    long jobStartTime = System.currentTimeMillis();
    if (args.length < 1) {
      LOG.error("Usage: ImporterMain <gcs-bucket-uri>");
      System.exit(1);
    }
    String gcsBackupPath = "gs://" + args[0] + "/";
    JobReport report = new JobReport(JobReport.JobType.IMPORT);
    MasterEnvironment masterEnv = null;
    TokenManager tokenManager = null;

    long namespaceTime = 0, pipelineTime = 0;
    AtomicInteger namespaceCount = new AtomicInteger(0);
    AtomicInteger pipelineCount = new AtomicInteger(0);
    LogAppenderInitializer logAppenderInitializer = null;

    try {
      // --- Use the MasterEnvironment pattern from AbstractServiceMain ---
      // --- Use the MasterEnvironment pattern from AbstractServiceMain ---
      CConfiguration cConf = CConfiguration.create();

      if (args.length > 2 && args[0].contains("-tp")) {
        String tenantProjectId = args[2];
        cConf.set("log.publisher.gcp_logging.project", tenantProjectId);
        LOG.info("Overriding log publisher project to tenant project '{}' for testing.", tenantProjectId);
      }

      Configuration hConf = new Configuration();
      File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
      if (hConfFile.exists()) {
        hConf.addResource(hConfFile.toURI().toURL());
      }
      // Assuming a kubernetes environment.
      masterEnv = MasterEnvironments.setMasterEnvironment(MasterEnvironments.create(cConf, "k8s"));
      MasterEnvironmentContext masterEnvContext = MasterEnvironments.createContext(cConf, hConf, masterEnv.getName());
      masterEnv.initialize(masterEnvContext);
      // --- End MasterEnvironment setup ---

      Injector injector = initializeInjector(cConf, hConf, masterEnv);

      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
          Constants.Logging.COMPONENT_NAME,
          Service.IMPORTER));

      // --- Start required services ---
      tokenManager = injector.getInstance(TokenManager.class);
      LOG.info("Starting TokenManager service...");
      tokenManager.startAndWait();
      LOG.info("TokenManager service started successfully.");
      // --- End service startup ---

      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      // Get the HTTP clients for all operations
      RemoteNamespaceClient namespaceClient = injector.getInstance(RemoteNamespaceClient.class);
      RemoteAppLifecycleClient appLifecycleClient = injector.getInstance(RemoteAppLifecycleClient.class);
      TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);

      report.open(locationFactory, gcsBackupPath);

      // Import Namespaces via HTTP Client
      long startTime = System.currentTimeMillis();
      List<NamespaceMeta> importedNamespaces = importNamespaces(namespaceClient, locationFactory, gcsBackupPath,
          report, namespaceCount);
      namespaceTime = System.currentTimeMillis() - startTime;

      // Import Pipelines via HTTP Client
      startTime = System.currentTimeMillis();
      importPipelines(importedNamespaces, appLifecycleClient,transactionRunner, locationFactory, gcsBackupPath,
          report, pipelineCount);
      pipelineTime = System.currentTimeMillis() - startTime;

      LOG.info("All import tasks finished.");

    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error during setup or execution.", e);
    }  finally {
      // --- Stop services in reverse order of startup ---
      if (tokenManager != null && tokenManager.isRunning()) {
        LOG.info("Stopping TokenManager service...");
        try {
          tokenManager.stopAndWait();
          LOG.info("TokenManager service stopped.");
        } catch (Exception e) {
          LOG.error("Failed to stop TokenManager service cleanly.", e);
        }
      }

      if (masterEnv != null) {
        masterEnv.destroy();
      }
      if (logAppenderInitializer != null) {
        logAppenderInitializer.close();
      }
      long jobEndTime = System.currentTimeMillis();
      if (report.isOpen()) {
        Map<String, String> summaryData = new LinkedHashMap<>();
        summaryData.put("Total Job Time (seconds)", String.format("%.2f", (jobEndTime - jobStartTime) / 1000.0));
        summaryData.put("---", "---");
        summaryData.put("Namespace Import Time (seconds)", String.format("%.2f", namespaceTime / 1000.0));
        summaryData.put("Namespace Count", namespaceCount.toString());
        summaryData.put("Pipeline Import Time (seconds)", String.format("%.2f", pipelineTime / 1000.0));
        summaryData.put("Pipeline Version Count", pipelineCount.toString());
        report.writeSummaryReport(summaryData);
      }
      report.close();
      LOG.info("==================== IMPORT SUMMARY ====================");
      LOG.info(String.format("Namespace Import:           %.2f seconds for %d namespaces",
          namespaceTime / 1000.0, namespaceCount.get()));
      LOG.info(String.format("Pipeline Import:            %.2f seconds for %d pipeline versions",
          pipelineTime / 1000.0, pipelineCount.get()));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Total Job Time:                  %.2f seconds", (jobEndTime - jobStartTime) / 1000.0));
      LOG.info("========================================================");
      LOG.info("Job finished.");
    }
  }

  private static Injector initializeInjector(CConfiguration cConf, Configuration hConf, MasterEnvironment masterEnv)
      throws MalformedURLException {
    LOG.info("Initializing Guice injector using MasterEnvironment pattern...");

    hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    hConf.setBoolean("fs.gs.auth.service.account.enable", true);

    List<Module> modules = new ArrayList<>(Arrays.asList(
        new ConfigModule(cConf, hConf),
        new IOModule(),
        new DFSLocationModule(),
        new StorageModule(),
        new AuthenticationContextModules().getMasterModule(),
        CoreSecurityRuntimeModule.getDistributedModule(cConf),
        new RemoteLogAppenderModule(),
        RemoteAuthenticatorModules.getDefaultModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
            bind(DiscoveryService.class)
                .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
            bind(DiscoveryServiceClient.class)
                .toProvider(
                    new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(RemoteAppLifecycleClient.class).in(Scopes.SINGLETON);
            bind(RemoteNamespaceClient.class).in(Scopes.SINGLETON);
          }
        }
    ));

    return Guice.createInjector(modules);
  }

  public static List<NamespaceMeta> importNamespaces(RemoteNamespaceClient namespaceClient, LocationFactory locationFactory,
      String backupPath, JobReport report,  AtomicInteger namespaceCounter) {
    LOG.info("Starting import of all namespaces via HTTP endpoint...");
    List<NamespaceMeta> importedNamespaces = new ArrayList<>();
    try {
      Location namespacesDir = locationFactory.create(backupPath).append("namespaces");
      if (!namespacesDir.exists()) {
        LOG.warn("Namespaces directory does not exist: {}", namespacesDir.toURI());
        return Collections.emptyList();
      }

      for (Location namespaceDir : namespacesDir.list()) {
        if (!namespaceDir.isDirectory()) {
          continue;
        }

        String namespaceId = namespaceDir.getName();
        try {
          Location namespaceFile = namespaceDir.append("namespaceMeta.json");
          if (!namespaceFile.exists()) {
            report.addFailure("Namespace", namespaceId, "N/A", "namespaceMeta.json not found");
            continue;
          }
          NamespaceMeta namespaceMeta;
          try (Reader reader = new InputStreamReader(namespaceFile.getInputStream(), StandardCharsets.UTF_8)) {
            namespaceMeta = GSON.fromJson(reader, NamespaceMeta.class);
          }
          namespaceClient.create(namespaceId, namespaceMeta);

          namespaceCounter.incrementAndGet();
          importedNamespaces.add(namespaceMeta);
          report.addSuccess("Namespace", namespaceId, "N/A", namespaceFile.toURI().toString());
        } catch (Exception e) {
          LOG.error("Failed to import namespace '{}'", namespaceId, e);
          report.addFailure("Namespace", namespaceId, "N/A", e.getMessage());
        }
      }
    } catch (IOException e) {
      LOG.error("FATAL: Failed to list contents of namespaces directory.", e);
    }
    return importedNamespaces;
  }

  public static void importPipelines(List<NamespaceMeta> namespaces,
      RemoteAppLifecycleClient appLifecycleClient, TransactionRunner transactionRunner,
      LocationFactory locationFactory, String backupPath,
      JobReport report, AtomicInteger pipelineCounter) {
    int numThreads = 25;
    LOG.info("Starting parallel import of all pipelines via AppFabric HTTP endpoint using {} threads...", numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (NamespaceMeta namespace : namespaces) {
      try {
        Location pipelinesBackupDir = locationFactory.create(backupPath).append("namespaces")
            .append(namespace.getName()).append("pipelines");

        if (!pipelinesBackupDir.exists()) {
          LOG.debug("No pipelines directory found for namespace '{}', skipping.", namespace.getName());
          continue;
        }

        for (Location pipelineDir : pipelinesBackupDir.list()) {
          if (!pipelineDir.isDirectory()) {
            continue;
          }
          Runnable pipelineDeploymentTask = createPipelineDeploymentTask(
              namespace.getName(), pipelineDir, appLifecycleClient, transactionRunner, report, pipelineCounter);
          futures.add(executor.submit(pipelineDeploymentTask));
        }
      } catch (IOException e) {
        LOG.error("FATAL: Failed to scan pipeline directories for namespace '{}'", namespace.getName(), e);
        report.addFailure("Pipeline-Batch", "ALL", namespace.getName(),
            "Failed to scan GCS directory: " + e.getMessage());
      }
    }

    LOG.info("All {} pipeline deployment tasks have been submitted. Waiting for completion...", futures.size());
    for (int i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).get();
        if ((i + 1) % 100 == 0) {
          LOG.info("Completed deployment for {}/{} pipelines...", i + 1, futures.size());
        }
      } catch (Exception e) {
        LOG.error("A pipeline deployment task failed.", e.getCause());
      }
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
        LOG.warn("Thread pool did not terminate gracefully after 5 minutes.");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting for thread pool to terminate.", e);
      executor.shutdownNow();
    }
    LOG.info("All pipeline deployment tasks have finished.");
  }

  private static Runnable createPipelineDeploymentTask(String namespaceName, Location pipelineDir,
      RemoteAppLifecycleClient appLifecycleClient, TransactionRunner transactionRunner, JobReport report,
      AtomicInteger pipelineVersionCounter) {
    return () -> {
      String pipelineName = pipelineDir.getName();
      try {
        List<Location> versionDirs = new ArrayList<>();
        Location latestVersionDir = null;
        ApplicationMeta latestAppMeta = null;

        // First pass: Find the latest version
        for (Location versionDir : pipelineDir.list()) {
          if (!versionDir.isDirectory()) continue;

          Location pipelineFile = versionDir.append("pipeline.json");
          if (!pipelineFile.exists()) continue;

          ApplicationMeta appMeta;
          try (Reader reader = new InputStreamReader(pipelineFile.getInputStream(), StandardCharsets.UTF_8)) {
            appMeta = GSON.fromJson(reader, ApplicationMeta.class);
          }

          if (appMeta.getChange() != null && appMeta.getChange().getLatest()) {
            latestVersionDir = versionDir;
            latestAppMeta = appMeta;
          } else {
            versionDirs.add(versionDir);
          }
        }

        // Import all historical versions first using the DAL
        for (Location versionDir : versionDirs) {
          writeHistoricalVersion(namespaceName, pipelineName, versionDir, transactionRunner, report, pipelineVersionCounter);
        }

        // Finally, deploy the latest version using the API to set it as 'latest'
        if (latestVersionDir != null) {
          deployLatestVersion(namespaceName, pipelineName, latestVersionDir, latestAppMeta, appLifecycleClient, report, pipelineVersionCounter);
        }

      } catch (IOException e) {
        LOG.error("Failed to list versions for pipeline '{}' in namespace '{}'", pipelineName, namespaceName, e);
        report.addFailure("Pipeline-Batch", pipelineName, namespaceName, "Failed to list versions: " + e.getMessage());
        throw new RuntimeException(e);
      }
    };
  }

  private static void writeHistoricalVersion(String namespaceName, String pipelineName, Location versionDir,
      TransactionRunner transactionRunner, JobReport report, AtomicInteger pipelineVersionCounter) {
    String versionName = versionDir.getName();
    String pipelineIdForReport = String.format("%s (historical version: %s)", pipelineName, versionName);
    try {
      Location pipelineFile = versionDir.append("pipeline.json");
      ApplicationMeta appMeta;
      try (Reader reader = new InputStreamReader(pipelineFile.getInputStream(), StandardCharsets.UTF_8)) {
        appMeta = GSON.fromJson(reader, ApplicationMeta.class);
      }

      ApplicationId appId = new ApplicationId(namespaceName, pipelineName, versionName);

      TransactionRunners.run(transactionRunner, context -> {
        AppMetadataStore appStore = AppMetadataStore.create(context);
        appStore.createApplicationVersion(appId, appMeta, false); // false because it's not latest
      });

      pipelineVersionCounter.incrementAndGet();
      report.addSuccess("Pipeline", pipelineIdForReport, namespaceName, pipelineFile.toURI().toString());
      LOG.info("Successfully wrote historical pipeline version {} for app {}", versionName, pipelineName);
    } catch (Exception e) {
      LOG.error("Failed to write historical pipeline version '{}'", pipelineIdForReport, e);
      report.addFailure("Pipeline", pipelineIdForReport, namespaceName, e.getMessage());
    }
  }

  private static void deployLatestVersion(String namespaceName, String pipelineName, Location versionDir, ApplicationMeta appMeta,
      RemoteAppLifecycleClient appLifecycleClient, JobReport report, AtomicInteger pipelineVersionCounter) {

    String versionName = versionDir.getName();
    String pipelineIdForReport = String.format("%s (latest version: %s)", pipelineName, versionName);

    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        AppRequest<Object> appRequest = new AppRequest<>(ArtifactSummary.from(appMeta.getSpec().getArtifactId()),
            GSON.fromJson(appMeta.getSpec().getConfiguration(), Object.class));

        appLifecycleClient.deploy(namespaceName, pipelineName, appRequest);
        LOG.info("Successfully deployed latest pipeline {} with version {} in namespace {}", pipelineName, versionName, namespaceName);

        pipelineVersionCounter.incrementAndGet();
        report.addSuccess("Pipeline", pipelineIdForReport, namespaceName, versionDir.toURI().toString());
        return; // Success, exit method

      } catch (Exception e) {
        boolean isRetryable = e instanceof RetryableException || e instanceof SocketTimeoutException ||
            (e.getMessage() != null && e.getMessage().contains("SocketTimeoutException")) ||
            (e.getMessage() != null && e.getMessage().contains("Directory not empty"));

        if (isRetryable && attempt < MAX_RETRIES - 1) {
          LOG.warn("Deploy failed for latest pipeline '{}', attempt {}/{}. Retrying...",
              pipelineIdForReport, attempt + 1, MAX_RETRIES, e);
          try {
            TimeUnit.MILLISECONDS.sleep(INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            report.addFailure("Pipeline", pipelineIdForReport, namespaceName, "Retry interrupted.");
            return;
          }
        } else {
          LOG.error("Failed to deploy latest pipeline version '{}' with a non-retryable error.",
              pipelineIdForReport, namespaceName, e);
          report.addFailure("Pipeline", pipelineIdForReport, namespaceName, e.getMessage());
          return; // Permanent failure, exit method
        }
      }
    }
  }
}