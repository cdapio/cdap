/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.relational;

import java.util.Set;

/**
 * Linear variant of {@link RelationalTransform} for plugins that has 1 input and produce 1 output
 * relation.
 */
public interface LinearRelationalTransform extends RelationalTransform {

  /**
   * Implementation of full interface that delegates to {@link #transform(RelationalTranformContext,
   * Relation)}.
   *
   * @param context tranformation context with engine, input and output parameters
   * @return true if the number of input relations is 1. False otherwise.
   */
  default boolean transform(RelationalTranformContext context) {
    Set<String> names = context.getInputRelationNames();
    if (names.size() != 1) {
      return false;
    }
    context.setOutputRelation(
        transform(context, context.getInputRelation(names.iterator().next())));
    return true;
  }

  /**
   * <p>This call will be done for every suitable engine until after supported tranformation is
   * declared or there is no more engines left.</p>
   * <p>A transformation is supported if plugin returned true, all registered output relations are
   * valid and engine can perform the transformation requested.</p>
   * <p>If no engine can perform a transformation, a fall back to regular by-row transformation is
   * performed.
   *
   * @param context tranformation context with engine, input and output parameters
   * @param input input relation, the only one available from the context
   * @return output relation. Plugin may return invalid relation to indicate unsupported
   *     transformation. Even if plugin returned a valid relation, engine may deny performing a
   *     transformation.
   * @see {@link LinearRelationalTransform} for simpler interface for plugins that take 1 input and
   *     produce 1 output.
   */
  Relation transform(RelationalTranformContext context, Relation input);

  /**
   * <p>For Linear Transformation, we need to union if there are multiple inputs as it takes only 1 input</p>
   * @return true
   */
  default boolean requireUnionInputs() {
    return true;
  }
}
