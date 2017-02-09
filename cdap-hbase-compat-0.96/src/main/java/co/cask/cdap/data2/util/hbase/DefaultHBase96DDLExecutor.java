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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.security.access.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link HBaseDDLExecutor} for HBase 0.96
 */
public class DefaultHBase96DDLExecutor extends DefaultHBaseDDLExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHBase96DDLExecutor.class);

  @Override
  public HTableDescriptor getHTableDescriptor(TableDescriptor descriptor) {
    return HBase96TableDescriptorUtil.getHTableDescriptor(descriptor);
  }

  @Override
  public TableDescriptor getTableDescriptor(HTableDescriptor descriptor) {
    return HBase96TableDescriptorUtil.getTableDescriptor(descriptor);
  }

  @Override
  protected void doGrantPermissions(String namespace, @Nullable String table,
                                    Map<String, Permission.Action[]> permissions) {
    // no-op, not called
  }

  @Override
  public void grantPermissions(String namespace, @Nullable String table, Map<String, String> permissions)
    throws IOException {
    StringBuilder statements = new StringBuilder();
    for (Map.Entry<String, String> entry : permissions.entrySet()) {
      String user = entry.getKey();
      String actions = entry.getValue();
      try {
        toActions(actions);
      } catch (IllegalArgumentException e) {
        String entity = table == null ? "namespace " + namespace : "table " + namespace + ":" + table;
        String userOrGroup = user.startsWith("@") ? "group " + user.substring(1) : "user " + user;
        throw new IOException(String.format("Invalid permissions '%s' for %s and %s: %s",
                                            actions, entity, userOrGroup, e.getMessage()));
      }
      statements.append(String.format("\ngrant '%s', '%s', '%s:%s'", user, actions.toUpperCase(), namespace, table));
    }
    LOG.warn("Granting permissions is not implemented for HBase 0.96. " +
               "Please grant these permissions manually in the hbase shell: {}", statements);
  }
}
