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
package co.cask.cdap.etl.transform;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.LookupTableConfig;

/**
 * {@link LookupProvider} implementation for {@link ScriptTransform}.
 */
public class ScriptLookupProvider {

  private final LookupProvider delegate;
  private final LookupConfig config;

  public ScriptLookupProvider(LookupProvider delegate, LookupConfig config) {
    this.delegate = delegate;
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  public ScriptLookup provide(String table, JavaTypeConverters converters) {
    if (config == null) {
      throw new RuntimeException("Missing lookup configuration");
    }

    LookupTableConfig tableConfig = config.getTable(table);
    if (tableConfig == null || tableConfig.getType() != LookupTableConfig.TableType.DATASET) {
      throw new RuntimeException(String.format("Dataset %s not declared in configuration", table));
    }

    DatasetProperties arguments = DatasetProperties.builder().addAll(tableConfig.getDatasetProperties()).build();
    return new ScriptLookup(delegate.provide(table, arguments.getProperties()), tableConfig, converters);
  }
}
