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

package co.cask.cdap.explore.service.hive;

import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryStatus;

import java.util.List;

/**
 * OperationInfo representing an inactive operation.
 */
final class InactiveOperationInfo extends OperationInfo {
  private final List<ColumnDesc> schema;
  private final QueryStatus status;

  InactiveOperationInfo(OperationInfo operationInfo, List<ColumnDesc> schema, QueryStatus status) {
    super(operationInfo.getSessionHandle(), operationInfo.getOperationHandle(),
          operationInfo.getSessionConf(), operationInfo.getStatement(),
          operationInfo.getTimestamp(), operationInfo.getNamespace(), operationInfo.isReadOnly());
    this.schema = schema;
    this.status = status;
  }

  public List<ColumnDesc> getSchema() {
    return schema;
  }

  @Override
  public QueryStatus getStatus() {
    return status;
  }
}
