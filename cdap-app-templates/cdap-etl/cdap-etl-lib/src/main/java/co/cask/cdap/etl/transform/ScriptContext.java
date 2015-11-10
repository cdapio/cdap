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

import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import org.slf4j.Logger;

import javax.annotation.Nullable;

/**
 * Context passed to {@link co.cask.cdap.etl.transform.ScriptTransform} and
 * {@link co.cask.cdap.etl.transform.ScriptFilterTransform} scripts.
 */
public class ScriptContext {
  private final Logger logger;

  private final StageMetrics metrics;
  private final ScriptLookupProvider lookup;
  private final JavaTypeConverters js;

  public ScriptContext(
    Logger logger, StageMetrics metrics, LookupProvider lookup, @Nullable LookupConfig lookupConfig,
    JavaTypeConverters js) {

    this.logger = logger;
    this.metrics = metrics;
    this.lookup = new ScriptLookupProvider(lookup, lookupConfig);
    this.js = js;
  }

  public Logger getLogger() {
    return logger;
  }

  public StageMetrics getMetrics() {
    return metrics;
  }

  public ScriptLookup getLookup(String table) {
    return lookup.provide(table, js);
  }
}
