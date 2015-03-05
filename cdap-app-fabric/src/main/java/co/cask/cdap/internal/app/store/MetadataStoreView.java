/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Store for application metadata
 */
public abstract class MetadataStoreView<ID extends Id, VALUE> {

  protected final MetadataStoreDataset metadataStore;
  protected final Class<VALUE> valueType;

  @Nullable
  protected final String partition;

  /**
   * Constructs a view of the {@link MetadataStoreDataset} which fetches VALUEs keyed by (partition, getKey()).
   *
   * @param metadataStore the {@link MetadataStoreDataset}
   * @param valueType type of object to store
   * @param partition partition within the {@link MetadataStoreDataset} to store VALUEs
   */
  public MetadataStoreView(MetadataStoreDataset metadataStore, Class<VALUE> valueType, @Nullable String partition) {
    this.metadataStore = metadataStore;
    this.partition = partition;
    this.valueType = valueType;
  }

  protected abstract void appendKey(MDSKey.Builder builder, ID id);

  protected void appendPartitionedKey(MDSKey.Builder builder, ID id) {
    if (partition != null) {
      builder.add(partition);
    }
    appendKey(builder, id);
  }

  protected MDSKey getKey(ID id) {
    MDSKey.Builder builder = new MDSKey.Builder();
    appendPartitionedKey(builder, id);
    return builder.build();
  }

//  protected List<VALUE> list() {
//    return metadataStore.list(getListKey(), valueType);
//  }
//
//  protected void deleteAll() {
//    metadataStore.deleteAll(getListKey());
//  }
//
//  protected MDSKey getListKey() {
//    MDSKey.Builder builder = new MDSKey.Builder();
//    if (partition != null) {
//      builder.add(partition);
//    }
//    return builder.build();
//  }

  public VALUE get(ID id) throws NotFoundException {
    return metadataStore.get(getKey(id), valueType);
  }

  public void create(ID id, VALUE value) throws AlreadyExistsException {
    assertNotExists(id);
    metadataStore.write(getKey(id), value);
  }

  public void update(ID id, VALUE value) throws NotFoundException {
    assertExists(id);
    metadataStore.write(getKey(id), value);
  }

  public void delete(ID id) throws NotFoundException {
    assertExists(id);
    metadataStore.deleteAll(getKey(id));
  }

  public boolean exists(ID id) {
    return metadataStore.exists(getKey(id));
  }

  public void assertExists(ID id) throws NotFoundException {
    if (!exists(id)) {
      throw new NotFoundException(id);
    }
  }

  public void assertNotExists(ID id) throws AlreadyExistsException {
    if (!exists(id)) {
      throw new AlreadyExistsException(id);
    }
  }
}
