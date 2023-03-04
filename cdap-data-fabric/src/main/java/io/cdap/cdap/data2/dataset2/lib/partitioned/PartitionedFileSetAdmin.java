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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.CompositeDatasetAdmin;
import java.util.Map;

/**
 * Implementation of {@link io.cdap.cdap.api.dataset.DatasetAdmin} for {@link
 * io.cdap.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDataset} instances.
 */
public class PartitionedFileSetAdmin extends CompositeDatasetAdmin {

  private final DatasetContext context;
  private final DatasetSpecification spec;

  public PartitionedFileSetAdmin(DatasetContext context, DatasetSpecification spec,
      Map<String, DatasetAdmin> embeddedAdmins) {
    super(embeddedAdmins);
    this.context = context;
    this.spec = spec;
  }
}
