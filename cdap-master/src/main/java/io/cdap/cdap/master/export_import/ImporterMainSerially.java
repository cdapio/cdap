package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.artifact.ArtifactSummary;
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
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImporterMainSerially {
  private static final Logger LOG = LoggerFactory.getLogger(ImporterMainSerially.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  public static void main(String[] args) throws IOException {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
        Constants.Logging.COMPONENT_NAME,
        Service.IMPORTER));

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

    try {
      // --- Use the MasterEnvironment pattern from AbstractServiceMain ---
      CConfiguration cConf = CConfiguration.create();
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

      report.open(locationFactory, gcsBackupPath);

      // Import Namespaces via HTTP Client
      long startTime = System.currentTimeMillis();
      List<NamespaceMeta> importedNamespaces = importNamespaces(namespaceClient, locationFactory, gcsBackupPath,
          report, namespaceCount);
      namespaceTime = System.currentTimeMillis() - startTime;

      // Import Pipelines via HTTP Client
      startTime = System.currentTimeMillis();
      importPipelines(importedNamespaces, appLifecycleClient, locationFactory, gcsBackupPath,
          report, pipelineCount);
      pipelineTime = System.currentTimeMillis() - startTime;

      LOG.info("All import tasks finished.");

    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error during setup or execution.", e);
      System.exit(1);
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
        // TransactionExecutorModule is removed as we are not accessing the DB directly.
        new AuthenticationContextModules().getMasterModule(),
        CoreSecurityRuntimeModule.getDistributedModule(cConf),
        RemoteAuthenticatorModules.getDefaultModule(),
        // This bridge module provides the DiscoveryService created by the MasterEnvironment to Guice.
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DiscoveryService.class)
                .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
            bind(DiscoveryServiceClient.class)
                .toProvider(
                    new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
          }
        },
        // Module to bind the required HTTP clients
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
          // Use the remote client to create the namespace.
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

  public static void importPipelines(List<NamespaceMeta> namespaces, RemoteAppLifecycleClient appLifecycleClient,
      LocationFactory locationFactory, String backupPath, JobReport report,
      AtomicInteger pipelineCounter) throws IOException {
    LOG.info("Starting import of all pipelines via AppFabric HTTP endpoint...");

    for (NamespaceMeta namespace : namespaces) {
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
        String pipelineName = pipelineDir.getName();

        try {
          List<Location> versionDirs = new ArrayList<>();
          for (Location versionDir : pipelineDir.list()) {
            if (versionDir.isDirectory()) {
              versionDirs.add(versionDir);
            }
          }
          Collections.sort(versionDirs, (l1, l2) -> {
            try {
              return Long.compare(l1.lastModified(), l2.lastModified());
            } catch (IOException e) {
              LOG.warn("Could not compare modification times for pipeline versions, sort order may be incorrect.", e);
              return 0;
            }
          });

          if (versionDirs.isEmpty()) {
            LOG.warn("No versions found for pipeline '{}' in namespace '{}'", pipelineName, namespace.getName());
            continue;
          }

          for (int i = 0; i < versionDirs.size(); i++) {
            Location versionDir = versionDirs.get(i);

            String originalVersionName = versionDir.getName();
            String pipelineIdForReport = String.format("%s (original version: %s)", pipelineName, originalVersionName);
            String sourcePath = "";

            try {
              Location pipelineFile = versionDir.append("pipeline.json");
              sourcePath = pipelineFile.toURI().toString();
              if (!pipelineFile.exists()) {
                report.addFailure("Pipeline", pipelineIdForReport, namespace.getName(), "pipeline.json not found");
                continue;
              }

              // --- FIX: Read ApplicationDetail instead of ApplicationMeta ---
              ApplicationDetail appDetail;
              try (Reader reader = new InputStreamReader(pipelineFile.getInputStream(), StandardCharsets.UTF_8)) {
                appDetail = GSON.fromJson(reader, ApplicationDetail.class);
              }

              // Construct the AppRequest from the ApplicationDetail object.
              ArtifactSummary artifactSummary = appDetail.getArtifact();
              Object appConfig = GSON.fromJson(appDetail.getConfiguration(), Object.class);

              AppRequest<Object> appRequest = new AppRequest<>(artifactSummary, appConfig, appDetail.getOwnerPrincipal());

              LOG.info("Deploying configuration for pipeline '{}' from original version '{}'",
                  pipelineName, originalVersionName);
              appLifecycleClient.deploy(namespace.getName(), pipelineName, appRequest);

              pipelineCounter.incrementAndGet();
              report.addSuccess("Pipeline", pipelineIdForReport, namespace.getName(), sourcePath);
              LOG.info("Successfully deployed configuration for pipeline '{}' from original version '{}'",
                  pipelineName, originalVersionName);

            } catch (Exception e) {
              LOG.error(
                  "Failed to deploy pipeline configuration '{}' from original version '{}' in namespace '{}'",
                  pipelineName, originalVersionName, namespace.getName(), e);
              report.addFailure("Pipeline", pipelineIdForReport, namespace.getName(),
                  e.getMessage());
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to process pipeline directory '{}' in namespace '{}'", pipelineName,
              namespace.getName(), e);
          report.addFailure("Pipeline-Batch", pipelineName, namespace.getName(),
              "Failed to list versions: " + e.getMessage());
        }
      }
    }
  }
}