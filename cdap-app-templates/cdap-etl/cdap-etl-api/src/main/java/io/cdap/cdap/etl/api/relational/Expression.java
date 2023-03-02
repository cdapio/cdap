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
 * Compiled scalar expression produced by {@link ExpressionFactory#compile} that can be passed to
 * various relational algebra calls of {@link Relation} objects. Note that relations, engine and
 * expression factories must come from the same {@link RelationalTranformContext} and are valid only
 * for the time of the {@link RelationalTransform#transform} call.
 */
public interface Expression {

  /**
   * @return if this expression is valid. If during expression generation, it's found to be
   *     unsupported or invalid, generation will return an invalid Expression. Any {@link Relation}
   *     operation with invalid expression will result in an invalid {@link Relation}
   * @see #getValidationError() on operation problem details
   */
  boolean isValid();

  /**
   * @return validation error if expression is not valid
   * @see #isValid()
   */
  String getValidationError();
}
