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

package io.cdap.cdap.spi.hbase;


import io.cdap.cdap.api.annotation.Beta;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface providing the HBase DDL operations. All methods except {@link #initialize} must be
 * idempotent in order to allow retry of failed operations.
 */
@Beta
public interface HBaseDDLExecutor extends Closeable {

  /**
   * Initialize the {@link HBaseDDLExecutor}. This method is called once when the executor is
   * created, before any other methods are called.
   *
   * @param context the context for the executor
   */
  void initialize(HBaseDDLExecutorContext context);

  /**
   * Create the specified namespace if it does not exist. This method gets called when CDAP attempts
   * to create a new namespace.
   *
   * @param name the namespace to create
   * @return whether the namespace was created
   * @throws IOException if a remote or network exception occurs
   */
  boolean createNamespaceIfNotExists(String name) throws IOException;

  /**
   * Delete the specified namespace if it exists. This method is called during namespace deletion
   * process.
   *
   * @param name the namespace to delete
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if there are tables in the namespace
   */
  void deleteNamespaceIfExists(String name) throws IOException;

  /**
   * Create the specified table if it does not exist. This method is called during the creation of
   * an HBase backed dataset (either system or user).
   *
   * @param descriptor the descriptor for the table to create
   * @param splitKeys the initial split keys for the table
   * @throws IOException if a remote or network exception occurs
   */
  void createTableIfNotExists(TableDescriptor descriptor, @Nullable byte[][] splitKeys)
      throws IOException;

  /**
   * Enable the specified table if it is disabled.
   *
   * @param namespace the namespace of the table to enable
   * @param name the name of the table to enable
   * @throws IOException if a remote or network exception occurs
   */
  void enableTableIfDisabled(String namespace, String name) throws IOException;

  /**
   * Disable the specified table if it is enabled.
   *
   * @param namespace the namespace of the table to disable
   * @param name the name of the table to disable
   * @throws IOException if a remote or network exception occurs
   */
  void disableTableIfEnabled(String namespace, String name) throws IOException;

  /**
   * Modify the specified table. This is called when an HBase backed dataset has its properties
   * modified. In order to modify the HBase table, CDAP first calls {@code disableTableIfEnabled},
   * then calls this method, then calls {@code enableTableIfDisabled}.
   *
   * @param namespace the namespace of the table to modify
   * @param name the name of the table to modify
   * @param descriptor the descriptor for the table
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void modifyTable(String namespace, String name, TableDescriptor descriptor) throws IOException;

  /**
   * Truncate the specified table. Implementation of this method should disable the table first. The
   * table must also be re-enabled by implementation at the end of truncate operation.
   *
   * @param namespace the namespace of the table to truncate
   * @param name the name of the table to truncate
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void truncateTable(String namespace, String name) throws IOException;

  /**
   * Delete the table if it exists. In order to delete the HBase table, CDAP first calls {@code
   * disableTableIfEnabled}, then calls this method.
   *
   * @param namespace the namespace of the table to delete
   * @param name the table to delete
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void deleteTableIfExists(String namespace, String name) throws IOException;

  /**
   * Grant permissions on a table or namespace to users or groups.
   *
   * @param namespace the namespace of the table
   * @param table the name of the. If null, then the permissions are applied to the namespace
   * @param permissions A map from user or group name to the permissions for that user or group,
   *     given as a string containing only characters 'a'(Admin), 'c'(Create), 'r'(Read),
   *     'w'(Write), and 'x'(Execute). Group names must be prefixed with the character '@'.
   * @throws IOException if anything goes wrong
   */
  void grantPermissions(String namespace, @Nullable String table, Map<String, String> permissions)
      throws IOException;
}
