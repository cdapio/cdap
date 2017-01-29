/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Explore client facade to be used by streams and datasets.
 */
public class ExploreFacade {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreFacade.class);

  private final ExploreClient exploreClient;
  private final boolean exploreEnabled;

  @Inject
  public ExploreFacade(ExploreClient exploreClient, CConfiguration cConf) {
    this.exploreClient = exploreClient;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    if (!exploreEnabled) {
      LOG.warn("Explore functionality for datasets is disabled. All calls to enable explore will be no-ops");
    }
  }

  /**
   * Enables ad-hoc exploration of the given stream.
   *
   * @param stream id of the stream.
   * @param tableName name of the Hive table to create.
   * @param format format of the stream events.
   */
  public void enableExploreStream(StreamId stream, String tableName,
                                  FormatSpecification format) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExploreStream(stream, tableName, format);
    handleExploreFuture(futureSuccess, "enable", "stream", stream.getStream());
  }

  /**
   * Disables ad-hoc exploration of the given stream.
   *
   * @param stream id of the stream.
   * @param tableName name of the Hive table to delete.
   */
  public void disableExploreStream(StreamId stream, String tableName) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExploreStream(stream, tableName);
    handleExploreFuture(futureSuccess, "disable", "stream", stream.getStream());
  }

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance id.
   */
  public void enableExploreDataset(DatasetId datasetInstance) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExploreDataset(datasetInstance);
    handleExploreFuture(futureSuccess, "enable", "dataset", datasetInstance.getDataset());
  }

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance id.
   * @param spec the dataset specification of the dataset
   * @param truncating whether this call to create() is part of a truncate() operation, which is in some
   *                   case implemented using disableExplore() followed by enableExplore()
   */
  public void enableExploreDataset(DatasetId datasetInstance,
                                   DatasetSpecification spec,
                                   boolean truncating) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExploreDataset(datasetInstance, spec, truncating);
    handleExploreFuture(futureSuccess, "enable", "dataset", datasetInstance.getDataset());
  }

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance id.
   * @param oldSpec the previous dataset spec
   */
  public void updateExploreDataset(DatasetId datasetInstance,
                                   DatasetSpecification oldSpec,
                                   DatasetSpecification newSpec) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.updateExploreDataset(datasetInstance, oldSpec, newSpec);
    handleExploreFuture(futureSuccess, "update", "dataset", datasetInstance.getDataset());
  }

  /**
   * Disable ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance id.
   */
  public void disableExploreDataset(DatasetId datasetInstance) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExploreDataset(datasetInstance);
    handleExploreFuture(futureSuccess, "disable", "dataset", datasetInstance.getDataset());
  }

  /**
   * Disable ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance id.
   * @param spec the dataset specification of the dataset
   */
  public void disableExploreDataset(DatasetId datasetInstance,
                                    DatasetSpecification spec) throws ExploreException, SQLException {
    if (!(exploreEnabled && isDatasetExplorable(datasetInstance))) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExploreDataset(datasetInstance, spec);
    handleExploreFuture(futureSuccess, "disable", "dataset", datasetInstance.getDataset());
  }

  public void addPartition(DatasetId datasetInstance, DatasetSpecification spec,
                           PartitionKey key, String location) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.addPartition(datasetInstance, spec, key, location);
    handleExploreFuture(futureSuccess, "add", "partition", datasetInstance.getDataset());
  }

  public void dropPartition(DatasetId datasetInstance, DatasetSpecification spec,
                            PartitionKey key) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.dropPartition(datasetInstance, spec, key);
    handleExploreFuture(futureSuccess, "drop", "partition", datasetInstance.getDataset());
  }

  public void createNamespace(NamespaceMeta namespace) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<ExploreExecutionResult> futureSuccess = exploreClient.addNamespace(namespace);
    handleExploreFuture(futureSuccess, "add", "namespace", namespace.getName());
  }

  public void removeNamespace(NamespaceId namespace) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<ExploreExecutionResult> futureSuccess = exploreClient.removeNamespace(namespace);
    handleExploreFuture(futureSuccess, "remove", "namespace", namespace.getNamespace());
  }

  //TODO: CDAP-4627 - Figure out a better way to identify system datasets in user namespaces
  // Same check is done in DatasetsUtil.isUserDataset method.
  private boolean isDatasetExplorable(DatasetId datasetInstance) {
    return !NamespaceId.SYSTEM.getNamespace().equals(datasetInstance.getNamespace()) &&
      !"system.queue.config".equals(datasetInstance.getDataset()) &&
      !datasetInstance.getDataset().startsWith("system.sharded.queue") &&
      !datasetInstance.getDataset().startsWith("system.queue") &&
      !datasetInstance.getDataset().startsWith("system.stream");
  }

  // wait for the enable/disable operation to finish and log and throw exceptions as appropriate if there was an error.
  private void handleExploreFuture(ListenableFuture future, String operation, String type, String name)
    throws ExploreException, SQLException {
    try {
      future.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Future interrupted", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof ExploreException) {
        LOG.error("{} operation did not finish successfully for {} instance {}.",
                  operation, type, name);
        throw (ExploreException) t;
      } else if (t instanceof SQLException) {
        throw (SQLException) t;
      } else if (t instanceof HandleNotFoundException) {
        // Cannot happen unless explore server restarted, or someone calls close in between.
        LOG.error("Error running {} explore", operation, e);
        throw Throwables.propagate(e);
      }
    } catch (TimeoutException e) {
      LOG.error("Error running {} explore - operation timed out", operation, e);
      throw Throwables.propagate(e);
    }
  }

}
