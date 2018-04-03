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
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;
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
public class ProvisionerStore {
  private static final DatasetId TABLE_ID = NamespaceId.SYSTEM.dataset("provisioner.meta");
  private static final byte[] subscriberPrefix = Bytes.toBytes("s:");
  private static final byte[] subscriberMessageCol = Bytes.toBytes("m");
  private final Table table;

  private ProvisionerStore(Table table) {
    this.table = table;
  }

  @Nullable
  public String getSubscriberState(String clientId) throws Exception {
    byte[] val = table.get(getSubscriberRowKey(clientId), subscriberMessageCol);
    return val == null ? null : Bytes.toString(val);
  }

  public void persistSubscriberState(String clientId, String lastFetchedId) throws Exception {
    table.put(getSubscriberRowKey(clientId), subscriberMessageCol, Bytes.toBytes(lastFetchedId));
  }

  private byte[] getSubscriberRowKey(String clientId) {
    return Bytes.concat(subscriberPrefix, Bytes.toBytes(clientId));
  }

  public static ProvisionerStore get(DatasetContext datasetContext) {
    Table table = datasetContext.getDataset(TABLE_ID.getNamespace(), TABLE_ID.getDataset());
    return new ProvisionerStore(table);
  }

  public static void createIfNotExists(DatasetFramework datasetFramework)
    throws IOException, DatasetManagementException {
    DatasetsUtil.getOrCreateDataset(datasetFramework, TABLE_ID, Table.class.getName(),
                                    DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS);
  }
}
