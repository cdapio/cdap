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

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupTableConfig;

import java.util.Set;

/**
 * Provides JavaScript-friendly lookup functions for {@link ScriptTransform}.
 * Used directly by {@link ScriptTransform}.
 */
public class ScriptLookup {

  private final Lookup<Object> delegate;
  private final JavaTypeConverters js;
  private final LookupTableConfig config;

  public ScriptLookup(Lookup<Object> delegate, LookupTableConfig config, JavaTypeConverters js) {
    this.config = config;
    this.js = js;
    this.delegate = config.isCacheEnabled() ? new CachingLookup<>(delegate, config.getCacheConfig()) : delegate;
  }

  public Object lookup(String key) {
    // TODO: CDAP-4171 handle ObjectMappedTable
    return delegate.lookup(key);
  }

  public Object lookup(String... keys) {
    return js.mapToJSObject(delegate.lookup(keys));
  }

  public Object lookup(Set<String> keys) {
    return js.mapToJSObject(delegate.lookup(keys));
  }
}
