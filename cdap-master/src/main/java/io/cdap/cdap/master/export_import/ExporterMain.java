package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
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
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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

/**
 * Main class for the export job, which exports namespaces and pipelines via internal HTTP APIs.
 */
public class ExporterMain {
  private static final Logger LOG = LoggerFactory.getLogger(ExporterMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
      new GsonBuilder().setPrettyPrinting()).create();

  public static void main(String[] args) throws IOException {
    long jobStartTime = System.currentTimeMillis();
    if (args.length < 1) {
      LOG.error("Usage: ExporterMain <gcs-bucket-uri>");
      System.exit(1);
    }
    String gcsBackupPath = "gs://" + args[0] + "/";
    JobReport report = new JobReport(JobReport.JobType.EXPORT);
    MasterEnvironment masterEnv = null;
    TokenManager tokenManager = null;

    long namespaceTime = 0, pipelineTime = 0;
    AtomicInteger namespaceCount = new AtomicInteger(0);
    AtomicInteger pipelineCount = new AtomicInteger(0);

    // LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Id.Namespace.SYSTEM.getId(),
    //     Constants.Logging.COMPONENT_NAME,
    //     Service.EXPORTER));

    try {
      CConfiguration cConf = CConfiguration.create();
      Configuration hConf = new Configuration();
      File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
      if (hConfFile.exists()) {
        hConf.addResource(hConfFile.toURI().toURL());
      }

      masterEnv = MasterEnvironments.setMasterEnvironment(MasterEnvironments.create(cConf, "k8s"));
      MasterEnvironmentContext masterEnvContext = MasterEnvironments.createContext(cConf, hConf, masterEnv.getName());
      masterEnv.initialize(masterEnvContext);

      Injector injector = initializeInjector(cConf, hConf, masterEnv);

      // Start the token manager to ensure internal authentication is initialized.
      tokenManager = injector.getInstance(TokenManager.class);
      LOG.info("Starting TokenManager service...");
      tokenManager.startAndWait();
      LOG.info("TokenManager service started successfully.");

      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
      RemoteFetchDataClient cdapClient = injector.getInstance(RemoteFetchDataClient.class);

      report.open(locationFactory, gcsBackupPath);

      // Export Namespaces
      long startTime = System.currentTimeMillis();
      List<NamespaceMeta> allNamespaces = exportNamespaces(cdapClient, locationFactory, gcsBackupPath,
          report, namespaceCount);
      namespaceTime = System.currentTimeMillis() - startTime;

      // Export Pipelines with pagination
      startTime = System.currentTimeMillis();
      exportPipelines(allNamespaces, cdapClient, locationFactory, gcsBackupPath,
          report, pipelineCount);
      pipelineTime = System.currentTimeMillis() - startTime;

    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error.", e);
      System.exit(1);
    } finally {
      if (tokenManager != null && tokenManager.isRunning()) {
        LOG.info("Stopping TokenManager service...");
        tokenManager.stopAndWait();
      }
      if (masterEnv != null) {
        masterEnv.destroy();
      }
      long jobEndTime = System.currentTimeMillis();
      if (report.isOpen()) {
        Map<String, String> summaryData = new LinkedHashMap<>();
        summaryData.put("Total Job Time (seconds)", String.format("%.2f", (jobEndTime - jobStartTime) / 1000.0));
        summaryData.put("---", "---");
        summaryData.put("Namespace Export Time (seconds)", String.format("%.2f", namespaceTime / 1000.0));
        summaryData.put("Namespace Count", namespaceCount.toString());
        summaryData.put("Pipeline Export Time (seconds)", String.format("%.2f", pipelineTime / 1000.0));
        summaryData.put("Pipeline Version Count", pipelineCount.toString());
        report.writeSummaryReport(summaryData);
      }
      report.close();
      LOG.info("==================== EXPORT SUMMARY ====================");
      LOG.info(String.format("Namespace Export: %.2f seconds for %d namespaces",
          namespaceTime / 1000.0, namespaceCount.get()));
      LOG.info(String.format("Pipeline Export:  %.2f seconds for %d pipeline versions",
          pipelineTime / 1000.0, pipelineCount.get()));
      LOG.info("------------------------------------------------------");
      LOG.info(String.format("Total Job Time:     %.2f seconds", (jobEndTime - jobStartTime) / 1000.0));
      LOG.info("========================================================");
    }
  }

  private static Injector initializeInjector(CConfiguration cConf, Configuration hConf, MasterEnvironment masterEnv)
      throws MalformedURLException {
    hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    hConf.setBoolean("fs.gs.auth.service.account.enable", true);

    List<Module> modules = new ArrayList<>(Arrays.asList(
        new ConfigModule(cConf, hConf),
        new IOModule(),
        new DFSLocationModule(),
        new AuthenticationContextModules().getMasterModule(),
        CoreSecurityRuntimeModule.getDistributedModule(cConf),
        RemoteAuthenticatorModules.getDefaultModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DiscoveryService.class)
                .toProvider(new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceSupplier()));
            bind(DiscoveryServiceClient.class)
                .toProvider(
                    new SupplierProviderBridge<>(masterEnv.getDiscoveryServiceClientSupplier()));
            bind(RemoteFetchDataClient.class).in(Scopes.SINGLETON);
          }
        }
    ));
    return Guice.createInjector(modules);
  }

  public static List<NamespaceMeta> exportNamespaces(RemoteFetchDataClient cdapClient, LocationFactory locationFactory,
      String backupPath, JobReport report, AtomicInteger namespaceCounter)
      throws Exception {
    LOG.info("Starting export of all namespaces...");
    List<NamespaceMeta> namespaces = cdapClient.listNamespaces();
    Location baseLocation = locationFactory.create(backupPath);
    baseLocation.mkdirs();
    Location namespacesDir = baseLocation.append("namespaces");

    for (NamespaceMeta namespace : namespaces) {
      namespaceCounter.incrementAndGet();
      String namespaceId = namespace.getName();
      try {
        Location namespaceLocation = namespacesDir.append(namespaceId).append("namespaceMeta.json");
        try (Writer writer = new OutputStreamWriter(namespaceLocation.getOutputStream(), StandardCharsets.UTF_8)) {
          GSON.toJson(namespace, writer);
        }
        report.addSuccess("Namespace", namespaceId, "N/A", namespaceLocation.toURI().toString());
      } catch (Exception e) {
        LOG.error("Failed to export namespace '{}'", namespaceId, e);
        report.addFailure("Namespace", namespaceId, "N/A", e.getMessage());
      }
    }
    LOG.info("Finished exporting {} namespaces.", namespaceCounter.get());
    return namespaces;
  }

  public static void exportPipelines(List<NamespaceMeta> allNamespaces, RemoteFetchDataClient cdapClient,
      LocationFactory locationFactory, String backupPath, JobReport report,
      AtomicInteger pipelineCounter) {
    LOG.info("Starting export of all pipeline versions with pagination...");
    for (NamespaceMeta namespace : allNamespaces) {
      String namespaceName = namespace.getName();
      LOG.info("Starting pipeline export for namespace '{}'.", namespaceName);
      try {
        String nextPageToken = null;
        boolean hasMorePages = true;

        while (hasMorePages) {
          LOG.debug("Fetching page of applications from namespace '{}' with token '{}'", namespaceName, nextPageToken);
          // Fetch one page of applications.
          RemoteFetchDataClient.PaginatedAppResponse response = cdapClient.listApplications(namespaceName, nextPageToken);
          List<ApplicationRecord> appRecords = response.getApplications();
          LOG.debug("Fetched {} applications in this page.", appRecords.size());

          for (ApplicationRecord appRecord : appRecords) {
            String appName = appRecord.getName();
            String appVersion = appRecord.getAppVersion();
            try {
              // Get the full detail for this specific version.
              ApplicationDetail appDetail = cdapClient.getApplicationDetail(namespaceName, appName, appVersion);

              // Per your excellent suggestion, we will export the ApplicationDetail object directly.
              // This is the correct approach as it contains all necessary information for the import
              // and avoids the bug of trying to access a non-existent AppSpecification.
              Location pipelineVersionDir = locationFactory.create(backupPath)
                  .append("namespaces").append(namespaceName)
                  .append("pipelines").append(appName).append(appVersion);
              pipelineVersionDir.mkdirs();

              Location pipelineFile = pipelineVersionDir.append("pipeline.json");
              try (Writer writer = new OutputStreamWriter(pipelineFile.getOutputStream(), StandardCharsets.UTF_8)) {
                GSON.toJson(appDetail, writer);
              }

              pipelineCounter.incrementAndGet();
              String pipelineIdForReport = String.format("%s (v%s)", appName, appVersion);
              report.addSuccess("Pipeline", pipelineIdForReport, namespaceName, pipelineFile.toURI().toString());
            } catch (Exception e) {
              LOG.error("Failed to export pipeline '{}/{}'", appName, appVersion, e);
              report.addFailure("Pipeline", String.format("%s (v%s)", appName, appVersion),
                  namespaceName, e.getMessage());
            }
          }

          // Prepare for the next iteration.
          nextPageToken = response.getNextPageToken();
          if (nextPageToken == null || nextPageToken.isEmpty()) {
            hasMorePages = false;
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to list applications for namespace '{}'", namespaceName, e);
        report.addFailure("Pipeline-Batch", "ALL", namespaceName, e.getMessage());
      }
    }
    LOG.info("Finished exporting {} pipeline versions.", pipelineCounter.get());
  }
}