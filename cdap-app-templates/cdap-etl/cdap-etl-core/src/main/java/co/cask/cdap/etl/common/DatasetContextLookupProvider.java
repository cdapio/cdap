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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.etl.api.Lookup;

import java.util.Map;

/**
 * Implementation of {@link AbstractLookupProvider} that uses {@link DatasetContext}.
 * This provides the raw {@link Lookup} without doing any transaction-wrapping for you,
 * and therefore should be used when executing lookup functions inside a transaction.
 */
public class DatasetContextLookupProvider extends AbstractLookupProvider {

  private final DatasetContext context;

  public DatasetContextLookupProvider(DatasetContext context) {
    this.context = context;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return getLookup(table, context.getDataset(table, arguments));
  }
}
