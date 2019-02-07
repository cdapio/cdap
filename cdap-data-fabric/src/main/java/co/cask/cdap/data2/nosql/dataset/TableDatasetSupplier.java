/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.nosql.dataset;

import co.cask.cdap.api.dataset.Dataset;

import java.io.IOException;
import java.util.Map;

/**
 * Interface to supply the dataset for entity tables.
 */
public interface TableDatasetSupplier {
  /**
   * @return the dataset for the given entity table name and arguments
   * @throws IOException on errors when instantiating the dataset for the entity table
   */
  <T extends Dataset> T getTableDataset(String name, Map<String, String> arguments) throws IOException;
}
