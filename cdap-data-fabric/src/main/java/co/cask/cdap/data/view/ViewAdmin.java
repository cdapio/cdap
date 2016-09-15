/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data.view;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.ViewSystemMetadataWriter;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.utils.ExploreTableNaming;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.inject.Inject;

import java.util.List;

/**
 * Performs view operations.
 */
public class ViewAdmin {

  private final ViewStore store;
  private final ExploreFacade explore;
  private final ExploreTableNaming naming;
  private final MetadataStore metadataStore;

  @Inject
  public ViewAdmin(ViewStore store, ExploreFacade explore, ExploreTableNaming naming,
                   MetadataStore metadataStore) {
    this.store = store;
    this.explore = explore;
    this.naming = naming;
    this.metadataStore = metadataStore;
  }

  public boolean createOrUpdate(StreamViewId viewId, ViewSpecification spec) throws Exception {
    try {
      ViewSpecification previousSpec = store.get(viewId);
      if (spec.getTableName() == null) {
        // use the previous table name
        spec = new ViewSpecification(spec.getFormat(), previousSpec.getTableName());
      } else if (!spec.getTableName().equals(previousSpec.getTableName())) {
        throw new IllegalArgumentException(String.format("Cannot change table name for view %s", viewId));
      }
      explore.disableExploreStream(viewId.getParent().toId(), previousSpec.getTableName());
    } catch (NotFoundException e) {
      // pass through
    }

    if (spec.getTableName() == null) {
      spec = new ViewSpecification(spec.getFormat(), naming.getTableName(viewId.toId()));
    }
    explore.enableExploreStream(viewId.getParent().toId(), spec.getTableName(), spec.getFormat());
    boolean result = store.createOrUpdate(viewId, spec);
    ViewSystemMetadataWriter systemMetadataWriter = new ViewSystemMetadataWriter(metadataStore, viewId, spec);
    systemMetadataWriter.write();
    return result;
  }

  public void delete(StreamViewId viewId) throws Exception {
    ViewSpecification spec = store.get(viewId);
    explore.disableExploreStream(viewId.getParent().toId(), spec.getTableName());
    store.delete(viewId);
    metadataStore.removeMetadata(viewId);
  }

  public List<StreamViewId> list(StreamId streamId) {
    return store.list(streamId);
  }

  public ViewSpecification get(StreamViewId viewId) throws NotFoundException {
    return store.get(viewId);
  }

  public boolean exists(StreamViewId viewId) {
    return store.exists(viewId);
  }
}
