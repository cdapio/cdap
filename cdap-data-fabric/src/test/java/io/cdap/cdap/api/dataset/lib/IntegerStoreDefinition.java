/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib;

import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.data2.dataset2.lib.table.ObjectStoreDataset;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class IntegerStoreDefinition
  extends CompositeDatasetDefinition<ObjectStoreDataset<Integer>> {

  public IntegerStoreDefinition(String name, DatasetDefinition<? extends KeyValueTable, ?> keyValueTableDefinition) {
    super(name, "table", keyValueTableDefinition);
  }

  @Override
  public ObjectStoreDataset<Integer> getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                                Map<String, String> arguments,
                                                ClassLoader classLoader) throws IOException {

    KeyValueTable table = getDataset(datasetContext, "table", spec, arguments, classLoader);

    try {
      return new IntegerStore(spec.getName(), table);
    } catch (UnsupportedTypeException e) {
      // shouldn't happen
      throw new IOException(e);
    }
  }

}
