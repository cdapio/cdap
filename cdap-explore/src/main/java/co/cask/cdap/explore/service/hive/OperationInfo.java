package co.cask.cdap.explore.service.hive;

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

import co.cask.cdap.proto.QueryStatus;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;

import java.io.File;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
* Helper class to store information about a Hive operation in progress.
*/
public abstract class OperationInfo {
  private final SessionHandle sessionHandle;
  private final OperationHandle operationHandle;
  private final Map<String, String> sessionConf;
  private final String statement;
  private final long timestamp;
  private final String namespace;
  /**
   * Indicates whether this operation changes Datasets or not.
   */
  private final boolean readOnly;
  private final Lock nextLock = new ReentrantLock();
  private final Lock previewLock = new ReentrantLock();

  private File previewFile;
  private QueryStatus status;

  OperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                Map<String, String> sessionConf, String statement, long timestamp,
                String namespace, boolean readOnly) {
    this.sessionHandle = sessionHandle;
    this.operationHandle = operationHandle;
    this.sessionConf = sessionConf;
    this.statement = statement;
    this.timestamp = timestamp;
    this.namespace = namespace;
    this.readOnly = readOnly;
  }

  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  public OperationHandle getOperationHandle() {
    return operationHandle;
  }

  public Map<String, String> getSessionConf() {
    return sessionConf;
  }

  public String getStatement() {
    return statement;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public File getPreviewFile() {
    return previewFile;
  }

  public void setPreviewFile(File previewFile) {
    this.previewFile = previewFile;
  }

  public Lock getNextLock() {
    return nextLock;
  }

  public Lock getPreviewLock() {
    return previewLock;
  }

  public String getNamespace() {
    return namespace;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public QueryStatus getStatus() {
    return status;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }
}
