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

package io.cdap.cdap.data2.util.hbase;

import com.google.common.base.Throwables;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.spi.hbase.TableDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link HBaseDDLExecutor} for HBase 1.0 CDH 5.5.0
 */
public class DefaultHBase10CDH550DDLExecutor extends DefaultHBaseDDLExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHBase10CDH550DDLExecutor.class);

  @Override
  public HTableDescriptor getHTableDescriptor(TableDescriptor descriptor) {
    return HBase10CDH550TableDescriptorUtil.getHTableDescriptor(descriptor);
  }

  @Override
  public TableDescriptor getTableDescriptor(HTableDescriptor descriptor) {
    return HBase10CDH550TableDescriptorUtil.getTableDescriptor(descriptor);
  }

  @Override
  protected void doGrantPermissions(String namespace, @Nullable String table,
      Map<String, Permission.Action[]> permissions) throws IOException {
    String entity = table == null ? "namespace " + namespace : "table " + namespace + ":" + table;
    try (Connection connection = ConnectionFactory.createConnection(admin.getConfiguration())) {
      if (!AccessControlClient.isAccessControllerRunning(connection)) {
        LOG.debug("Access control is off. Not granting privileges for {}. ", entity);
        return;
      }
      for (Map.Entry<String, Permission.Action[]> entry : permissions.entrySet()) {
        String user = entry.getKey();
        String userOrGroup = user.startsWith("@") ? "group " + user.substring(1) : "user " + user;
        Permission.Action[] actions = entry.getValue();
        try {
          LOG.info("Granting {} for {} to {}", Arrays.toString(actions), entity, userOrGroup);
          if (table != null) {
            AccessControlClient.grant(connection, TableName.valueOf(namespace, table), user, null,
                null, actions);
          } else {
            AccessControlClient.grant(connection, namespace, user, actions);
          }
        } catch (Throwable t) {
          Throwables.propagateIfInstanceOf(t, IOException.class);
          throw new IOException(String.format("Error while granting %s for %s to %s",
              Arrays.toString(actions), entity, userOrGroup), t);
        }
      }
    }
  }
}
