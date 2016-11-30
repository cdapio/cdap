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

import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;

import java.util.Map;

/**
 * OperationInfo that represents a read-write operation.
 */
final class ReadWriteOperationInfo extends OperationInfo {
  ReadWriteOperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                         Map<String, String> sessionConf, String statement, String hiveDatabase) {
    super(sessionHandle, operationHandle, sessionConf, statement, System.currentTimeMillis(), hiveDatabase, false);
  }
}
