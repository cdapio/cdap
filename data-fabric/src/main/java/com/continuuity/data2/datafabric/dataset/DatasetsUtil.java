package com.continuuity.data2.datafabric.dataset;

import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.InstanceConflictException;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Has handy methods for dealing with Datasets.
 * todo: once we have couple methods, refactor out from "util" into smth more sensible
 */
public final class DatasetsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetsUtil.class);

  private DatasetsUtil() {}

  /**
   * Gets instance of {@link Dataset}, while add instance to {@link DatasetManager} and creating the physical data set
   * if that one doesn't exist.
   * NOTE: does poor job guarding against races, i.e. only one client for this dataset instance is supported at a time
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetManager datasetManager,
                                                   String instanceName, String typeName,
                                                   DatasetInstanceProperties props, ClassLoader cl)
    throws DatasetManagementException, IOException {
    // making sure dataset instance is added
    DatasetAdmin admin = datasetManager.getAdmin(instanceName, cl);
    if (admin == null) {
      try {
        datasetManager.addInstance(typeName, instanceName, props);
      } catch (InstanceConflictException e) {
        // Do nothing: someone created this instance in between, just continuing
      } catch (DatasetManagementException e) {
        LOG.error("Could NOT add dataset instance {} of type {} with props {}",
                  instanceName, typeName, props, e);
        throw Throwables.propagate(e);
      }
      admin = datasetManager.getAdmin(instanceName, cl);
    }

    try {
      if (!admin.exists()) {
        admin.create();
      }
    } finally {
      admin.close();
    }

    return (T) datasetManager.getDataset(instanceName, null);
  }


}
