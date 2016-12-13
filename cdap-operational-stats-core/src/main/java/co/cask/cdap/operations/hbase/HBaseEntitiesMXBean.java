/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.hbase;

import javax.management.MXBean;

/**
 * {@link MXBean} representing HBase entity (namespaces, tables, snapshots) statistics.
 */
public interface HBaseEntitiesMXBean {
  /**
   * Returns the number of namespaces in HBase.
   */
  int getNamespaces();

  /**
   * Returns the number of tables in HBase.
   */
  int getTables();

  /**
   * Returns the number of completed table snapshots.
   */
  int getSnapshots();
}
