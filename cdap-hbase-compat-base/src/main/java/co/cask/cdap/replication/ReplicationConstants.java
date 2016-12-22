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

package co.cask.cdap.replication;

/**
 * Constants for Replication
 */
public class ReplicationConstants {

  /**
   * Constants for Replication Status Tool
   */
  public static final class ReplicationStatusTool {
    public static final String REPLICATION_STATE_TABLE_NAME = "hbase.replicationtable.name";
    public static final String REPLICATION_STATE_TABLE_DEFAULT_NAME = "replicationstate";
    public static final String REPLICATION_STATE_TABLE_NAMESPACE = "hbase.replicationtable.namespace";
    public static final String REPLICATION_STATE_TABLE_DEFAULT_NAMESPACE = "system";
    public static final String TIME_FAMILY = "t";
    public static final String REPLICATE_TIME_ROW_TYPE = "r";
    public static final String WRITE_TIME_ROW_TYPE = "w";
    public static final String REPLICATION_PERIOD = "hbase.replicationtable.updateperiod";
    public static final String REPLICATION_DELAY = "hbase.replicationtable.updatedelay";
    public static final long REPLICATION_PERIOD_DEFAULT = 20000; //20s
    public static final long REPLICATION_DELAY_DEFAULT = 30000; //30s
  }
}
