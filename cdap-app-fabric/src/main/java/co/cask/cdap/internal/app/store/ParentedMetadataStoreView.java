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
import com.google.common.base.Preconditions;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Store for application metadata
 */
public abstract class ParentedMetadataStoreView<PARENT_ID extends Id, ID extends Id, VALUE>
  extends MetadataStoreView<ID, VALUE> {

  private final MetadataStoreView<PARENT_ID, ?> parent;

  /**
   * Constructs a view of the {@link MetadataStoreDataset} which fetches VALUEs keyed by (partition, getKey()).
   *
   * @param metadataStore the {@link MetadataStoreDataset}
   * @param valueType type of object to store
   * @param partition partition within the {@link MetadataStoreDataset} to store VALUEs
   * @param parent the parent view
   */
  public ParentedMetadataStoreView(MetadataStoreDataset metadataStore, Class<VALUE> valueType,
                                   @Nullable String partition, MetadataStoreView<PARENT_ID, ?> parent) {
    super(metadataStore, valueType, partition);
    Preconditions.checkNotNull(parent);
    this.parent = parent;
  }

  protected abstract PARENT_ID getParent(ID id);

  protected void appendPartitionedKey(MDSKey.Builder builder, ID id) {
    parent.appendPartitionedKey(builder, getParent(id));
    super.appendPartitionedKey(builder, id);
  }

  protected List<VALUE> list(PARENT_ID parentId) throws NotFoundException {
    parent.assertExists(parentId);
    return metadataStore.list(getListKey(parentId), valueType);
  }

  protected void deleteAll(PARENT_ID parentId) throws NotFoundException {
    parent.assertExists(parentId);
    metadataStore.deleteAll(getListKey(parentId));
  }

  protected MDSKey getListKey(PARENT_ID parentId) {
    MDSKey.Builder builder = new MDSKey.Builder();
    parent.appendPartitionedKey(builder, parentId);
    if (partition != null) {
      builder.add(partition);
    }
    return builder.build();
  }

  protected MDSKey getKey(ID id) {
    MDSKey.Builder builder = new MDSKey.Builder();
    appendPartitionedKey(builder, id);
    return builder.build();
  }

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
