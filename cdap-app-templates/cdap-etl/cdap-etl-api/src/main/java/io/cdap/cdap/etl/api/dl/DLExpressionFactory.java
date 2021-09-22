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

package io.cdap.cdap.etl.api.dl;

import java.util.Set;

public interface DLExpressionFactory {
  Set<DLExpressionCapability> getCapabilities();
  DLExpression compile(String expression);

  /**
   * Available with {@link CoreDLExpressionCapabilities#CAN_GET_QUALIFIED_DATASET_NAME}
   */
  default String getQualifiedDataSetName(DLDataSet dataSet) {
    throw new UnsupportedOperationException();
  }

  /**
   * Available with {@link CoreDLExpressionCapabilities#CAN_GET_QUALIFIED_COLUMN_NAME}
   */
  default String getQualifiedColumnName(DLDataSet dataSet, String column) {
    throw new UnsupportedOperationException();
  }

  /**
   * Available with {@link CoreDLExpressionCapabilities#CAN_SET_DATASET_ALIAS}
   */
  default DLDataSet setDataSetAlias(DLDataSet dataSet, String alias) {
    throw new UnsupportedOperationException();
  }
}
