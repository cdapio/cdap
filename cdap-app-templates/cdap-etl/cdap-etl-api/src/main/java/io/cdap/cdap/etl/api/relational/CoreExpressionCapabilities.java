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
 * This enum defines core expression factory capabilities
 */
public enum CoreExpressionCapabilities implements Capability {
  /**
   * Set if {@link ExpressionFactory#setDataSetAlias(Relation, String)} is supported
   */
  CAN_SET_DATASET_ALIAS,
  /**
   * Set if {@link ExpressionFactory#getQualifiedDataSetName(Relation)} is supported
   */
  CAN_GET_QUALIFIED_DATASET_NAME,
  /**
   * Set if {@link ExpressionFactory#getQualifiedColumnName(Relation, String)} is supported
   */
  CAN_GET_QUALIFIED_COLUMN_NAME,
  /**
   * Set if expression factory automatically calculates lineage for it's expressions. As soon as
   * transformations are done using factory that implements this capability, plugin does not need to
   * provide lineage information by itself. Note: TODO CDAP-18609: currently no factories have this
   * capability
   */
  CALCULATES_LINEAGE
}
