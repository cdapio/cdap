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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * A {@link TransformContext} for use on the client side in the prepareRun method.
 * In this context, users are not allowed access resources localized for this pipeline.
 */
public class ClientTransformContext extends AbstractTransformContext {
  public ClientTransformContext(PluginContext pluginContext, Metrics metrics, LookupProvider lookup, String stageId) {
    super(pluginContext, metrics, lookup, stageId);
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    throw new UnsupportedOperationException("Access to local files is not permitted in prepareRun.");
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    throw new UnsupportedOperationException("Access to local files is not permitted in prepareRun.");
  }
}
