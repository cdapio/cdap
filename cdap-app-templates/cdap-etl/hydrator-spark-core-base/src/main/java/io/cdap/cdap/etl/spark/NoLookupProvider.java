/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark;

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;

import java.util.Map;

/**
 * A LookupProvider that doesn't work because lookups don't work in Spark.
 */
public class NoLookupProvider implements LookupProvider {
  public static final LookupProvider INSTANCE = new NoLookupProvider();

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    throw new UnsupportedOperationException("Lookup is not supported in Spark pipelines.");
  }
}
