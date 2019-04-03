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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.LookupProvider;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.List;
import java.util.Map;

/**
 * Base implementation of {@link TransformContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractTransformContext extends AbstractStageContext implements TransformContext {

  private final LookupProvider lookup;

  protected AbstractTransformContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec, LookupProvider lookup) {
    super(pipelineRuntime, stageSpec);
    this.lookup = lookup;
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return lookup.provide(table, arguments);
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }
}
