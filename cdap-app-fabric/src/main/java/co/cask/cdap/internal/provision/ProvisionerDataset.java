/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stores information used for provisioning.
 *
 * Stores subscriber offset information for TMS, cluster information for program runs, and state information for
 * each provision and deprovision operation.
 *
 * Subscriber information is stored as:
 *
 * rowkey           column
 * s:[client-id]    m -> [last fetched message id]
 *
 * This store does not wrap it's operations in a transaction. It is up to the caller to decide what operations
 * belong in a transaction.
 */
public class ProvisionerDataset {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final DatasetId TABLE_ID = NamespaceId.SYSTEM.dataset("app.meta");
  private static final byte[] SUBSCRIBER_PREFIX = Bytes.toBytes("pr.offset");
  private static final byte[] STATE_PREFIX = Bytes.toBytes("pr.state");
  private final MetadataStoreDataset table;

  public static ProvisionerDataset get(DatasetContext datasetContext) {
    Table table = datasetContext.getDataset(TABLE_ID.getNamespace(), TABLE_ID.getDataset());
    return new ProvisionerDataset(table);
  }

  public static void createIfNotExists(DatasetFramework datasetFramework)
    throws IOException, DatasetManagementException {
    DatasetsUtil.getOrCreateDataset(datasetFramework, TABLE_ID, Table.class.getName(),
                                    DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS);
  }

  private ProvisionerDataset(Table table) {
    this.table = new MetadataStoreDataset(table, GSON);
  }

  @Nullable
  public String getSubscriberState(String clientId) {
    return table.get(getSubscriberRowKey(clientId), String.class);
  }

  public void persistSubscriberState(String clientId, String lastFetchedId) {
    table.write(getSubscriberRowKey(clientId), lastFetchedId);
  }

  public List<ClusterInfo> listClusterInfo() {
    return table.list(new MDSKey.Builder().add(STATE_PREFIX).build(), ClusterInfo.class);
  }

  @Nullable
  public ClusterInfo getClusterInfo(ProgramRunId programRunId) {
    return table.get(getStateRowKey(programRunId), ClusterInfo.class);
  }

  public void putClusterInfo(ClusterInfo provisionerOperation) {
    table.write(getStateRowKey(provisionerOperation.getProgramRunId()), provisionerOperation);
  }

  public void deleteClusterInfo(ProgramRunId programRunId) {
    table.delete(getStateRowKey(programRunId));
  }

  private MDSKey getStateRowKey(ProgramRunId programRunId) {
    return new MDSKey.Builder().add(STATE_PREFIX)
      .add(programRunId.getNamespace())
      .add(programRunId.getApplication())
      .add(programRunId.getVersion())
      .add(programRunId.getType().name())
      .add(programRunId.getProgram())
      .add(programRunId.getRun())
      .build();
  }

  private MDSKey getSubscriberRowKey(String clientId) {
    return new MDSKey.Builder().add(SUBSCRIBER_PREFIX).add(clientId).build();
  }
}
