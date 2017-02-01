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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.spi.hbase.TableDescriptor;
import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Implementation of the {@link HBaseDDLExecutor} for HBase 1.2 CDH 5.7.0
 */
public class DefaultHBase12CDH570DDLExecutor extends DefaultHBaseDDLExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHBase12CDH570DDLExecutor.class);

  @Override
  public HTableDescriptor getHTableDescriptor(TableDescriptor descriptor) {
    return HBase12CDH570TableDescriptorUtil.getHTableDescriptor(descriptor);
  }

  @Override
  public TableDescriptor getTableDescriptor(HTableDescriptor descriptor) {
    return HBase12CDH570TableDescriptorUtil.getTableDescriptor(descriptor);
  }

  @Override
  protected void doGrantPermissions(String namespace, String name,
                                    Map<String, Permission.Action[]> permissions) throws IOException {
    TableName table = TableName.valueOf(namespace, name);
    try (Connection connection = ConnectionFactory.createConnection(admin.getConfiguration())) {
      if (!AccessControlClient.isAccessControllerRunning(connection)) {
        LOG.debug("Access control is off. Not granting privileges. ");
        return;
      }
      for (Map.Entry<String, Permission.Action[]> entry : permissions.entrySet()) {
        String user = entry.getKey();
        Permission.Action[] actions = entry.getValue();
        LOG.info("Granting {} for table {}:{} and user {}", Arrays.toString(actions), namespace, name, user);
        try {
          AccessControlClient.grant(connection, table, user, null, null, actions);
        } catch (Throwable t) {
          Throwables.propagateIfInstanceOf(t, IOException.class);
          throw new IOException(String.format("Error while granting %s for table %s:%s and user %s",
                                              Arrays.toString(actions), namespace, name, user), t);
        }
      }
    }
  }
}
