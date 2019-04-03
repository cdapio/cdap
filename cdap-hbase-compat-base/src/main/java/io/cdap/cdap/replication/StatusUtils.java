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

import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Utility Functions for Replication Status Coprocessors
 */
public final class StatusUtils {

  private StatusUtils() {
  }

  public static String getReplicationStateTableName(Configuration conf) throws IOException {
    String name = conf.get(ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_NAME);
    String ns =
      conf.get(ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_NAMESPACE);
    String nsPrefix = conf.get(Constants.Dataset.TABLE_PREFIX);
    String tableName =
      (nsPrefix != null) ? nsPrefix : "cdap"
        + "_"
        + (ns != null ? ns : ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_DEFAULT_NAMESPACE)
        + ":"
        + (name != null ? name : ReplicationConstants.ReplicationStatusTool.REPLICATION_STATE_TABLE_DEFAULT_NAME);
    return tableName;
  }
}


