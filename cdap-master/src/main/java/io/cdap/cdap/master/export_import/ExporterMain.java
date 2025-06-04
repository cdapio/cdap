package io.cdap.cdap.master.export_import;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import kafka.utils.Json;
import com.google.inject.Guice;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.cdap.cdap.common.conf.CConfiguration;
import com.google.inject.Injector;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.proto.NamespaceMeta;

public class ExporterMain {

  private static final Logger LOG = (Logger) LoggerFactory.getLogger(ExporterMain.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  public static void main(String[] args) {
    LOG.info("Starting CDAP Namespace Export Job using LocationFactory...");
    // LOG.debug("Received arguments: {}", Arrays.toString(args));

    // if (args.length < 1) {
    //   LOG.error("Usage: ExportJobMain <gcs-bucket-uri>");
    //   LOG.error("Example: ExportJobMain gs://my-backup-bucket/run-123");
    //   System.exit(1);
    // }

    try {
      Injector injector = initializeInjector();
      // TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
      LocationFactory locationFactory = injector.getInstance(LocationFactory.class);

      // --- Execute the Export Logic ---
      // exportNamespaces(transactionRunner, locationFactory, gcsBackupPath);
      exportNamespaces(locationFactory);

      LOG.info("Job finished successfully.");
    } catch (Exception e) {
      LOG.error("Job failed with an unrecoverable error.", e);
      System.exit(1);
    }
  }

  /**
   * Sets up the Guice injector with all necessary modules.
   */
  private static Injector initializeInjector() throws MalformedURLException {
    LOG.info("Initializing Guice injector...");

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();
    File hConfFile = new File("/etc/hadoop/conf/core-site.xml");
    if (hConfFile.exists()) {
      hConf.addResource(hConfFile.toURI().toURL());
      LOG.info("Loaded hConf from {}", hConfFile.getAbsolutePath());
    }

    // hConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // The following property is needed to enable service account authentication via Workload Identity.
    hConf.setBoolean("fs.gs.auth.service.account.enable", true);
    List<Module> modules = new ArrayList<>(Arrays.asList(
        new ConfigModule(cConf,hConf),
        new StorageModule(),
        new DFSLocationModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        }
    ));
    // Injector injector = Guice.createInjector(
    //     new DataFabricModules().getStandaloneModules(),
    //     new DataSetServiceModules().getStandaloneModules(),
    //     new TransactionExecutorModule(),
    //     new DFSLocationModule(), // <-- This module provides the LocationFactory
    //     binder -> {
    //       binder.bind(CConfiguration.class).toInstance(cConf);
    //       binder.bind(Configuration.class).toInstance(hConf);
    //     }
    // );
    Injector injector = Guice.createInjector(modules);

    LOG.info("Guice injector created successfully.");
    return injector;
  }

  /**
   * Fetches all namespaces and uploads their metadata to a GCS location.
   */
  public static void exportNamespaces(LocationFactory locationFactory) throws Exception {
    String backupPath = "gs://useast1-temp-bucket/";
    LOG.info("Starting export of namespaces to base path: {}", backupPath);
    System.out.println("Starting export");

    // List<NamespaceMeta> namespaces = TransactionRunners.run(transactionRunner, context -> {
    //   NamespaceTable namespaceTable = new NamespaceTable(context);
    //   return namespaceTable.list();
    // });
    //
    // LOG.info("Found {} namespaces to export.", namespaces.size());

    // Create the base location from the argument string
    Location baseLocation = locationFactory.create(backupPath);
    baseLocation.mkdirs(); // Ensure base directory exists

    // for (NamespaceMeta namespace : namespaces) {
    //   String namespaceId = namespace.getName();
    //   LOG.debug("Processing namespace '{}'...", namespaceId);

      // Get a child location for this specific namespace's metadata
    NamespaceMeta dummyNamespace = new NamespaceMeta.Builder()
        .setName("dummyNamespace")
        .setDescription("This is a dummy namespace for the export POC.")
        .build();
      Location namespaceLocation = baseLocation.append(dummyNamespace.getName()).append("namespaceMeta.json");

      // Use try-with-resources to automatically close the stream
      try (OutputStream outputStream = namespaceLocation.getOutputStream();
          Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {

        // Convert the NamespaceMeta object to a JSON string and write it

        GSON.toJson(dummyNamespace, writer);
      }

      LOG.info("Successfully exported namespace '{}' to {}", dummyNamespace, namespaceLocation.toURI());
    // }

    LOG.info("Finished exporting all namespaces.");
  }

}
