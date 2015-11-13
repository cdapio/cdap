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
package co.cask.cdap.etl.common;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.lookup.KeyValueTableLookup;

import javax.annotation.Nullable;

/**
 * {@link Lookup} that provides common functionality.
 */
public abstract class AbstractLookupProvider implements LookupProvider {

  @SuppressWarnings("unchecked")
  protected <T> Lookup<T> getLookup(String table, @Nullable Dataset dataset) {
    if (dataset == null) {
      throw new RuntimeException(String.format("Dataset %s does not exist", table));
    }

    if (dataset instanceof KeyValueTable) {
      return (Lookup<T>) new KeyValueTableLookup((KeyValueTable) dataset);
    } else {
      throw new RuntimeException(String.format("Dataset %s does not support lookup", table));
    }
  }
}
