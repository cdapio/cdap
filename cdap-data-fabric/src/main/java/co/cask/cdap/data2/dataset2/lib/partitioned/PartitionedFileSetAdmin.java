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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.id.DatasetId;
import com.google.inject.Provider;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of {@link co.cask.cdap.api.dataset.DatasetAdmin} for
 * {@link co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset} instances.
 */
public class PartitionedFileSetAdmin extends CompositeDatasetAdmin {
  private final DatasetContext context;
  private final DatasetSpecification spec;
  private final Provider<ExploreFacade> exploreFacadeProvider;

  public PartitionedFileSetAdmin(DatasetContext context, DatasetSpecification spec,
                                 Provider<ExploreFacade> exploreProvider,
                                 Map<String, DatasetAdmin> embeddedAdmins) {
    super(embeddedAdmins);
    this.context = context;
    this.spec = spec;
    this.exploreFacadeProvider = exploreProvider;
  }

  @Override
  public void truncate() throws IOException {
    super.truncate();
    // after underlying datasets are truncated, we need to clean up any existing Hive partitions
    // NOTE: if an error occurs below it may leave the dataset unexplorable, but re-issuing the command
    // should fix this.
    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        DatasetId instanceId = new DatasetId(context.getNamespaceId(), spec.getName());
        try {
          exploreFacade.disableExploreDataset(instanceId, spec);
          exploreFacade.enableExploreDataset(instanceId, spec);
        } catch (Exception e) {
          throw new DataSetException(String.format(
              "Unable to reset explore on dataset %s", instanceId), e);
        }
      }
    }
  }
}
