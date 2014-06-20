package com.continuuity.data2.datafabric.dataset.service.mds;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.dataset2.SingleTypeModule;
import com.continuuity.data2.dataset2.tx.TransactionalDatasetRegistry;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Map;

/**
 * Allows transactional operations with datasets metadata.
 */
public class MDSDatasetsRegistry extends TransactionalDatasetRegistry<MDSDatasets> {
  private final Map<String, ? extends DatasetModule> defaultModules;
  private final DatasetFramework dsFramework;

  @Inject
  public MDSDatasetsRegistry(TransactionSystemClient txClient,
                             @Named("defaultDatasetModules")
                             Map<String, ? extends DatasetModule> defaultModules,
                             @Named("datasetMDS") DatasetFramework framework,
                             CConfiguration conf) {
    super(txClient);
    this.defaultModules = defaultModules;
    this.dsFramework =
      new NamespacedDatasetFramework(framework, new ReactorDatasetNamespace(conf, DataSetAccessor.Namespace.SYSTEM));

  }

  @Override
  public void startUp() throws Exception {
    for (Map.Entry<String, ? extends DatasetModule> moduleEntry : defaultModules.entrySet()) {
      dsFramework.addModule(moduleEntry.getKey(), moduleEntry.getValue());
    }

    dsFramework.addModule("typeMDSModule", new SingleTypeModule(DatasetTypeMDS.class));
    dsFramework.addModule("instanceMDSModule", new SingleTypeModule(DatasetInstanceMDS.class));

    DatasetsUtil.createIfNotExists(dsFramework, "datasets.instance",
                                                DatasetInstanceMDS.class.getName(),
                                                DatasetProperties.EMPTY);

    DatasetsUtil.createIfNotExists(dsFramework, "datasets.type",
                                              DatasetTypeMDS.class.getName(),
                                              DatasetProperties.EMPTY);
  }

  @Override
  public void shutDown() throws Exception {
  }

  @Override
  protected MDSDatasets createContext() throws IOException, DatasetManagementException {
    Map<String, Dataset> datasets = ImmutableMap.of(
      // "null" for class being in system classpath, for mds it is always true
      "datasets.instance", dsFramework.getDataset("datasets.instance", null),
      "datasets.type", dsFramework.getDataset("datasets.type", null)
    );

    return new MDSDatasets(datasets);
  }
}
