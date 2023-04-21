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

/**
 * An analytical or transformation plugin can implement this interface to provide an alternative
 * declarative way of doing transformations. If relational transform call is successfull, the
 * transform will be passed to the underlying engine and plugin will not be called to transform each
 * row. If relational transform is not successful (no engines are available, engines don't have
 * capabilities or can't perform the requested transformation), fall back to per-row transformation
 * will be performed.
 */
public interface RelationalTransform {

  /**
   * This call allows to prefilter engines before doing {@link #transform}.
   *
   * @param engine engine to check
   * @return if this engine can be used for transform requested
   */
  default boolean canUseEngine(Engine engine) {
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
   * @return if plugin could perform a transformation. If true is returned, plugin must set all
   *     outputs to relations. Plugin may still set any output to invalid relation and return true.
   *     In this case transformation would be considered unsupported. Even if plugin returned true
   *     and set all outputs to valid relations, engine may deny performing a transformation.
   * @see {@link LinearRelationalTransform} for simpler interface for plugins that take 1 input and
   *     produce 1 output.
   */
  boolean transform(RelationalTranformContext context);

  /**
   * <p>This will union multiple input data together if required</p>
   * @return false
   */
  default boolean requireUnionInputs() {
    return false;
  }
}
