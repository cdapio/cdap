/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework;

/**
 * Identifier for CDAP Logging Context
 */
public class LogPathIdentifier {
  private static final String META_SEPARATOR = ":";
  private final String namespaceId;
  private final String pathId1;
  private final String pathId2;

  public LogPathIdentifier(String namespaceId, String pathId1, String pathId2) {
    this.namespaceId = namespaceId;
    this.pathId1 = pathId1;
    this.pathId2 = pathId2;
  }

  /**
   * NamespaceId String
   * @return namespace string
   */
  public String getNamespaceId() {
    return namespaceId;
  }

  /**
   * first part of path id
   * @return first path id of logging context in a namespace
   */
  String getPathId1() {
    return pathId1;
  }

  /**
   * second part of path id - used by {@link LogFileManager} to create directory
   * @return second path id of logging context in a namespace
   */
  String getPathId2() {
    return pathId2;
  }

  /**
   * Rowkey combining the namespace and log file identifier separated by separator ":"
   * @return rowkey string
   */
  public String getRowKey() {
    return String.format("%s%s%s%s%s", namespaceId, META_SEPARATOR, pathId1, META_SEPARATOR, pathId2);
  }
}
