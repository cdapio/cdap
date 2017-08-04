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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.spi.hbase.HBaseDDLExecutorContext;
import co.cask.cdap.spi.hbase.TableDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.access.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link HBaseDDLExecutor}.
 */
public abstract class DefaultHBaseDDLExecutor implements HBaseDDLExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHBaseDDLExecutor.class);
  private static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  protected Admin admin;

  @Override
  public void initialize(HBaseDDLExecutorContext context) {
    try {
      this.admin = ConnectionFactory.createConnection((Configuration) context.getConfiguration()).getAdmin();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create HBaseAdmin.", e);
    }
  }

  /**
   * Encode a HBase entity name to ASCII encoding using {@link URLEncoder}.
   *
   * @param entityName entity string to be encoded
   * @return encoded string
   */
  private String encodeHBaseEntity(String entityName) {
    try {
      return URLEncoder.encode(entityName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      throw new RuntimeException(e);
    }
  }

  private boolean hasNamespace(String name) throws IOException {
    Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
    Preconditions.checkArgument(name != null, "Namespace should not be null.");
    try {
      admin.getNamespaceDescriptor(encodeHBaseEntity(name));
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean createNamespaceIfNotExists(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Namespace should not be null.");
    if (hasNamespace(name)) {
      return false;
    }
    NamespaceDescriptor namespaceDescriptor =
      NamespaceDescriptor.create(encodeHBaseEntity(name)).build();
    admin.createNamespace(namespaceDescriptor);
    return true;
  }

  @Override
  public void deleteNamespaceIfExists(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Namespace should not be null.");
    if (hasNamespace(name)) {
      admin.deleteNamespace(encodeHBaseEntity(name));
    }
  }

  protected abstract HTableDescriptor getHTableDescriptor(TableDescriptor descriptor);

  protected abstract TableDescriptor getTableDescriptor(HTableDescriptor descriptor);

  @Override
  public void createTableIfNotExists(TableDescriptor descriptor, @Nullable byte[][] splitKeys)
    throws IOException {
    HTableDescriptor htd = getHTableDescriptor(descriptor);
    if (admin.tableExists(htd.getTableName())) {
      return;
    }

    boolean tableExistsFailure = false;
    try {
      LOG.debug("Attempting to create table '{}' if it does not exist", Bytes.toString(htd.getName()));
      admin.createTable(htd, splitKeys);
    } catch (TableExistsException e) {
      // table may exist because someone else is creating it at the same
      // time. But it may not be available yet, and opening it might fail.
      LOG.debug("Table '{}' already exists.", Bytes.toString(htd.getName()), e);
      tableExistsFailure = true;
    }

    // Wait for table to materialize
    try {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();
      long sleepTime = TimeUnit.MILLISECONDS.toNanos(5000L) / 10;
      sleepTime = sleepTime <= 0 ? 1 : sleepTime;
      do {
        if (admin.tableExists(htd.getTableName())) {
          if (tableExistsFailure) {
            LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.",
                     Bytes.toString(htd.getName()));
          } else {
            LOG.info("Table '{}' created.", Bytes.toString(htd.getName()));
          }
          return;
        } else {
          TimeUnit.NANOSECONDS.sleep(sleepTime);
        }
      } while (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < 5000L);
    } catch (InterruptedException e) {
      LOG.warn("Sleeping thread interrupted.");
    }
    LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", Bytes.toString(htd.getName()),
              MAX_CREATE_TABLE_WAIT);
  }

  @Override
  public void enableTableIfDisabled(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");

    try {
      admin.enableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    } catch (TableNotDisabledException e) {
      LOG.debug("Attempt to enable already enabled table {} in the namespace {}.", name, namespace);
    }
  }

  @Override
  public void disableTableIfEnabled(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");

    try {
      admin.disableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    } catch (TableNotEnabledException e) {
      LOG.debug("Attempt to disable already disabled table {} in the namespace {}.", name, namespace);
    }
  }

  @Override
  public void modifyTable(String namespace, String name, TableDescriptor descriptor)
    throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");
    Preconditions.checkArgument(descriptor != null, "Descriptor should not be null.");

    HTableDescriptor htd = getHTableDescriptor(descriptor);
    admin.modifyTable(htd.getTableName(), htd);
  }

  @Override
  public void truncateTable(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");

    HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    TableDescriptor tbd = getTableDescriptor(descriptor);
    disableTableIfEnabled(namespace, name);
    deleteTableIfExists(namespace, name);
    createTableIfNotExists(tbd, null);
  }

  @Override
  public void deleteTableIfExists(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");
    admin.deleteTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
  }

  @Override
  public void close() throws IOException {
    if (admin != null) {
      admin.close();
    }
  }

  protected abstract void doGrantPermissions(String namespace, @Nullable String table,
                                             Map<String, Permission.Action[]> permissions) throws IOException;

  @Override
  public void grantPermissions(String namespace, String table, Map<String, String> permissions) throws IOException {
    Map<String, Permission.Action[]> privilegesToGrant = new HashMap<>(permissions.size());
    for (Map.Entry<String, String> entry : permissions.entrySet()) {
      String user = entry.getKey();
      String actionsForUser = entry.getValue();
      try {
        privilegesToGrant.put(user, toActions(actionsForUser));
      } catch (IllegalArgumentException e) {
        String entity = table == null ? "namespace " + namespace : "table " + namespace + ":" + table;
        String userOrGroup = user.startsWith("@") ? "group " + user.substring(1) : "user " + user;
        throw new IOException(String.format("Error granting permissions '%s' for %s to %s: %s",
                                            actionsForUser, entity, userOrGroup, e.getMessage()));
      }
    }
    doGrantPermissions(namespace, table, privilegesToGrant);
  }

  protected Permission.Action[] toActions(String permissions) {
    Permission.Action[] actions = new Permission.Action[permissions.length()];
    for (int i = 0; i < actions.length; i++) {
      actions[i] = toAction(permissions.charAt(i));
    }
    return actions;
  }

  private Permission.Action toAction(char c) {
    switch (Character.toLowerCase(c)) {
      case 'a':
        return Permission.Action.ADMIN;
      case 'c':
        return Permission.Action.CREATE;
      case 'r':
        return Permission.Action.READ;
      case 'w':
        return Permission.Action.WRITE;
      case 'x':
        return Permission.Action.EXEC;
      default:
        throw new IllegalArgumentException(String.format("Unknown Action '%s'", c));
    }
  }
}
