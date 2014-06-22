package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.InstanceConflictException;
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
   * Gets instance of {@link Dataset}, while add instance to
   * {@link com.continuuity.data2.dataset2.DatasetFramework} and creating the physical data set
   * if that one doesn't exist.
   * NOTE: does poor job guarding against races, i.e. only one client for this dataset instance is supported at a time
   */
  public static <T extends Dataset> T getOrCreateDataset(DatasetFramework datasetFramework,
                                                   String instanceName, String typeName,
                                                   DatasetProperties props, ClassLoader cl)
    throws DatasetManagementException, IOException {

    createIfNotExists(datasetFramework, instanceName, typeName, props);
    return (T) datasetFramework.getDataset(instanceName, null);
  }

  /**
   * Creates instance of the data set if not exists
   */
  public static void createIfNotExists(DatasetFramework datasetFramework,
                                       String instanceName, String typeName,
                                       DatasetProperties props) throws DatasetManagementException, IOException {

    if (!datasetFramework.hasInstance(instanceName)) {
      try {
        datasetFramework.addInstance(typeName, instanceName, props);
      } catch (InstanceConflictException e) {
        // Do nothing: someone created this instance in between, just continuing
      } catch (DatasetManagementException e) {
        LOG.error("Could NOT add dataset instance {} of type {} with props {}",
                  instanceName, typeName, props, e);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Performs an upgrade of a dataset instance.
   */
  public static void upgradeDataset(DatasetFramework datasetFramework, String instanceName, ClassLoader cl)
    throws Exception {
    DatasetAdmin admin = datasetFramework.getAdmin(instanceName, cl);
    admin.upgrade();
  }
}
