/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.cdap;

import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.store.NamespaceTable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.cdap.cdap.proto.NamespaceMeta;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class ExportJobMainTemp
{
    private final static Logger LOG = LoggerFactory.getLogger(ExportJobMainTemp.class);
    private static final Gson GSON = new Gson();
    public void exportNamespaces(TransactionRunner transactionRunner, String bucketName, Storage gcsClient) {
        // CConfiguration cConf = CConfiguration.create();
        // List<Module> modules = new ArrayList<>(Arrays.asList(
        //     new ConfigModule(cConf),
        //     new StorageModule(),
        //     new AbstractModule() {
        //       @Override
        //       protected void configure() {
        //         bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
        //             .in(Scopes.SINGLETON);
        //       }
        //     }
        // ));
        // Injector injector = Guice.createInjector(modules);
        // TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
        LOG.debug("Starting export of namespaces");

        try {
            TransactionRunners.run(transactionRunner, context -> {
                NamespaceTable namespaceTable = new NamespaceTable(context);
                List<NamespaceMeta> namespaces = namespaceTable.list();
                LOG.info("Found {} namespaces to export.", namespaces.size());

                // Loop through each namespace and upload its metadata.
                for (NamespaceMeta namespace : namespaces) {
                    String namespaceId = namespace.getName();
                    LOG.debug("Processing namespace '{}'...", namespaceId);

                    // Convert the NamespaceMeta object to a JSON string.
                    String namespaceJson = GSON.toJson(namespace);

                    // Define the full path for the object in GCS.
                    String gcsObjectPath = String.format("cdap/namespaces/%s/namespaceMeta", namespaceId);

                    // Prepare the object for upload.
                    BlobId blobId = BlobId.of(bucketName, gcsObjectPath);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();

                    // Upload the JSON content as bytes.
                    gcsClient.create(blobInfo, namespaceJson.getBytes(StandardCharsets.UTF_8));

                    LOG.info("Successfully exported namespace '{}' to gs://{}/{}",
                        namespaceId, bucketName, gcsObjectPath);
                }
            }, Exception.class);
        } catch (Exception e) {
            LOG.error("Failed to export namespaces due to an unexpected transaction error.", e);
            // In a real K8s job, you might want the pod to fail. Throwing a RuntimeException
            // will cause the Java process to exit with a non-zero status code.
            throw new RuntimeException("Namespace export failed", e);
        }
        // TransactionRunners.run(transactionRunner, context -> {
        //   NamespaceTable namespaceTable = new NamespaceTable(context);
        //   List<NamespaceMeta> namespaces = namespaceTable.list();
        //   LOG.debug("Found {} namespaces: {}", namespaces.size(), namespaces);
        // });
        LOG.debug("Finished exporting namespaces.");
    }
    public static void main( String[] args )
    {
        LOG.debug("Args: {}", args);
        CConfiguration cConf = CConfiguration.create();
        List<Module> modules = new ArrayList<>(Arrays.asList(
            new ConfigModule(cConf),
            new StorageModule(),
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                        .in(Scopes.SINGLETON);
                }
            }
        ));
        Injector injector = Guice.createInjector(modules);
        TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
        String gcsBucket = args[0];
        Storage gcsClient;
        try {
            gcsClient = StorageOptions.getDefaultInstance().getService();
            LOG.info("Successfully initialized Google Cloud Storage client.");
        } catch (Exception e) {
            LOG.error("Failed to initialize GCS client. Please check authentication.", e);
            return;
        }
        ExportJobMainTemp exportJob = new ExportJobMainTemp();
        exportJob.exportNamespaces(transactionRunner, gcsBucket, gcsClient);
        System.out.println("Job finished.");
    }
}
