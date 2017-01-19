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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of the {@link HBaseDDLExecutor}.
 */
public abstract class DefaultHBaseDDLExecutor implements HBaseDDLExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(DefaultHBaseDDLExecutor.class);
  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.
  private final Configuration hConf;

  public DefaultHBaseDDLExecutor(Configuration hConf) {
    this.hConf = hConf;
  }

  /**
   * Encode a HBase entity name to ASCII encoding using {@link URLEncoder}.
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
  
  private boolean hasNamespace(HBaseAdmin admin, String name) throws IOException {
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
  public void createNamespaceIfNotExists(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Namespace should not be null.");
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (!hasNamespace(admin, name)) {
        NamespaceDescriptor namespaceDescriptor =
          NamespaceDescriptor.create(encodeHBaseEntity(name)).build();
        admin.createNamespace(namespaceDescriptor);
      }
    }
  }

  @Override
  public void deleteNamespaceIfExists(String name) throws IOException {
    Preconditions.checkArgument(name != null, "Namespace should not be null.");
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (hasNamespace(admin, name)) {
        admin.deleteNamespace(encodeHBaseEntity(name));
      }
    }
  }

  public abstract HTableDescriptor getHTableDescriptor(TableDescriptor descriptor);

  public abstract TableDescriptor getTableDescriptor(HTableDescriptor descriptor);

  @Override
  public void createTableIfNotExists(TableDescriptor descriptor, @Nullable byte[][] splitKeys)
    throws IOException {
    HTableDescriptor htd = getHTableDescriptor(descriptor);
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      if (admin.tableExists(htd.getName())) {
        return;
      }
      try {
        LOG.debug("Attempting to create table '{}' if it does not exist", Bytes.toString(htd.getName()));
        admin.createTable(htd, splitKeys);
        LOG.info("Table created '{}'", Bytes.toString(htd.getName()));
      } catch (TableExistsException e) {
        // table may exist because someone else is creating it at the same
        // time. But it may not be available yet, and opening it might fail.
        LOG.debug("Table '{}' already exists.", Bytes.toString(htd.getName()), e);
      }

      // Wait for table to materialize
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        long sleepTime = TimeUnit.MILLISECONDS.toNanos(5000L) / 10;
        sleepTime = sleepTime <= 0 ? 1 : sleepTime;
        do {
          if (admin.tableExists(descriptor.getName())) {
            LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.",
                     Bytes.toString(htd.getName()));
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
  }

  @Override
  public void enableTableIfDisabled(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");

    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      admin.enableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    } catch (TableNotDisabledException e) {
      LOG.debug("Attempt to enable already enabled table {} in the namespace {}.", name, namespace);
    }
  }

  @Override
  public void disableTableIfEnabled(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");

    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
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
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      HTableDescriptor htd = getHTableDescriptor(descriptor);
      admin.modifyTable(htd.getTableName(), htd);
    }
  }

  @Override
  public void truncateTable(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(namespace, encodeHBaseEntity(name)));
      TableDescriptor tbd = getTableDescriptor(descriptor);
      disableTableIfEnabled(namespace, name);
      deleteTableIfExists(namespace, name);
      createTableIfNotExists(tbd, null);
    }
  }

  @Override
  public void deleteTableIfExists(String namespace, String name) throws IOException {
    Preconditions.checkArgument(namespace != null, "Namespace should not be null");
    Preconditions.checkArgument(name != null, "Table name should not be null.");
    try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
      admin.deleteTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    }
  }
}
