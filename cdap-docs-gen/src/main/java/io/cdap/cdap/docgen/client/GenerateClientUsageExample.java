/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.docgen.client;

import com.google.common.collect.Maps;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.client.DatasetModuleClient;
import io.cdap.cdap.client.DatasetTypeClient;
import io.cdap.cdap.client.MonitorClient;
import io.cdap.cdap.client.PreferencesClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ServiceClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Generates cdap-docs/reference-manual/source/java-client-api.rst.
 */
public class GenerateClientUsageExample {

  private final ClientConfig clientConfig = null;

  public void applicationClient() throws Exception {
    // Construct the client used to interact with CDAP
    ApplicationClient appClient = new ApplicationClient(clientConfig);

    // Fetch the list of applications
    List<ApplicationRecord> apps = appClient.list(NamespaceId.DEFAULT);

    // Deploy an application
    File appJarFile = new File("your-app.jar");
    appClient.deploy(NamespaceId.DEFAULT, appJarFile);

    // Delete an application
    appClient.deleteApp(NamespaceId.DEFAULT.app("Purchase"));

    // List programs belonging to an application
    appClient.listPrograms(NamespaceId.DEFAULT.app("Purchase"));
  }

  public void preferencesClient() throws Exception {
    // Construct the client used to interact with CDAP
    PreferencesClient preferencesClient = new PreferencesClient(clientConfig);

    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("k1", "v1");

    // Set preferences at the Instance level
    preferencesClient.setInstancePreferences(propMap);

    // Get preferences at the Instance level
    preferencesClient.getInstancePreferences();

    // Delete preferences at the Instance level
    preferencesClient.deleteInstancePreferences();

    // Set preferences of MyApp application which is deployed in the Dev namespace
    preferencesClient.setApplicationPreferences(new ApplicationId("Dev", "MyApp"), propMap);

    // Get only the preferences of MyApp application which is deployed in the Dev namespace
    Map<String, String> appPrefs = preferencesClient.getApplicationPreferences(
      new ApplicationId("Dev", "MyApp"), false);

    // Get the resolved preferences (collapsed with higher level(s) of preferences)
    Map<String, String> resolvedAppPrefs = preferencesClient.getApplicationPreferences(
      new ApplicationId("Dev", "MyApp"), true);
  }

  public void programClient() throws Exception {
    // Construct the client used to interact with CDAP
    ProgramClient programClient = new ProgramClient(clientConfig);

    // Start a service in the WordCount example
    programClient.start(NamespaceId.DEFAULT.app("WordCount").service("RetrieveCounts"));

    // Fetch live information from the HelloWorld example
    // Live info includes the address of an component’s container host and the container’s debug port,
    // formatted in JSON
    programClient.getLiveInfo(NamespaceId.DEFAULT.app("HelloWorld").service("greet"));

    // Fetch program logs in the WordCount example
    programClient.getProgramLogs(NamespaceId.DEFAULT.app("WordCount").service("RetrieveCounts"), 0, Long.MAX_VALUE);

    // Scale a service in the HelloWorld example
    programClient.setServiceInstances(NamespaceId.DEFAULT.app("HelloWorld").service("greet"), 3);

    // Stop a service in the HelloWorld example
    programClient.stop(NamespaceId.DEFAULT.app("HelloWorld").service("greet"));
  }

  public void datasetClient() throws Exception {
    // Construct the client used to interact with CDAP
    DatasetClient datasetClient = new DatasetClient(clientConfig);

    // Fetch the list of datasets
    List<DatasetSpecificationSummary> datasets = datasetClient.list(NamespaceId.DEFAULT);

    // Create a dataset
    DatasetId datasetId = NamespaceId.DEFAULT.dataset("someDataset");
    datasetClient.create(datasetId, "someDatasetType");

    // Truncate a dataset
    datasetClient.truncate(datasetId);

    // Delete a dataset
    datasetClient.delete(datasetId);
  }

  public void datasetModuleClient() throws Exception {
    // Construct the client used to interact with CDAP
    DatasetModuleClient datasetModuleClient = new DatasetModuleClient(clientConfig);

    // Add a dataset module
    File moduleJarFile = createAppJarFile(SomeDatasetModule.class);
    DatasetModuleId datasetModuleId = NamespaceId.DEFAULT.datasetModule("someDatasetModule");
    datasetModuleClient.add(datasetModuleId, SomeDatasetModule.class.getName(), moduleJarFile);

    // Fetch the dataset module information
    DatasetModuleMeta datasetModuleMeta = datasetModuleClient.get(datasetModuleId);

    // Delete all dataset modules
    datasetModuleClient.deleteAll(NamespaceId.DEFAULT);
  }

  public void datasetTypeClient() throws Exception {
    // Construct the client used to interact with CDAP
    DatasetTypeClient datasetTypeClient = new DatasetTypeClient(clientConfig);

    // Fetch the dataset type information using the type name
    DatasetTypeMeta datasetTypeMeta = datasetTypeClient.get(NamespaceId.DEFAULT.datasetType("someDatasetType"));

    // Fetch the dataset type information using the classname
    datasetTypeClient.get(NamespaceId.DEFAULT.datasetType(SomeDataset.class.getName()));
  }

  public void serviceClient() throws Exception {
    // Construct the client used to interact with CDAP
    ServiceClient serviceClient = new ServiceClient(clientConfig);

    // Fetch service information using the service in the PurchaseApp example
    ServiceSpecification serviceSpec = serviceClient.get(
      NamespaceId.DEFAULT.app("PurchaseApp").service("CatalogLookup"));
  }

  public void monitorClient() throws Exception {
    // Construct the client used to interact with CDAP
    MonitorClient monitorClient = new MonitorClient(clientConfig);

    // Fetch the list of system services
    List<SystemServiceMeta> services = monitorClient.listSystemServices();

    // Fetch status of system transaction service
    String serviceStatus = monitorClient.getSystemServiceStatus("transaction");

    // Fetch the number of instances of the system transaction service
    int systemServiceInstances = monitorClient.getSystemServiceInstances("transaction");

    // Set the number of instances of the system transaction service
    monitorClient.setSystemServiceInstances("transaction", 1);
  }

  private File createAppJarFile(Class<?> cls) {
    return null;
  }

  /**
   *
   */
  private class SomeDatasetModule {

  }

  /**
   *
   */
  private class SomeDataset {

  }
}
