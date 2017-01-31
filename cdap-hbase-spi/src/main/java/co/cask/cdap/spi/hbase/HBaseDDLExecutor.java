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

package co.cask.cdap.spi.hbase;


import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Interface providing the HBase DDL operations.
 */
public interface HBaseDDLExecutor extends Closeable {
  /**
   * Initialize the {@link HBaseDDLExecutor}.
   * @param context the context for the executor
   */
  void initialize(HBaseDDLExecutorContext context);

  /**
   * Create the specified namespace if it does not exist.
   *
   * @param name the namespace to create
   * @throws IOException if a remote or network exception occurs
   */
  void createNamespaceIfNotExists(String name) throws IOException;

  /**
   * Delete the specified namespace if it exists.
   *
   * @param name the namespace to delete
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if there are tables in the namespace
   */
  void deleteNamespaceIfExists(String name) throws IOException;

  /**
   * Create the specified table if it does not exist.
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
   * Modify the specified table. The table must be disabled.
   *
   * @param namespace the namespace of the table to modify
   * @param name the name of the table to modify
   * @param descriptor the descriptor for the table
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void modifyTable(String namespace, String name, TableDescriptor descriptor) throws IOException;

  /**
   * Truncate the specified table. The table must be disabled.
   *
   * @param namespace the namespace of the table to truncate
   * @param name the name of the table to truncate
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void truncateTable(String namespace, String name) throws IOException;

  /**
   * Delete the table if it exists. The table must be disabled.
   *
   * @param namespace the namespace of the table to delete
   * @param name the table to delete
   * @throws IOException if a remote or network exception occurs
   * @throws IllegalStateException if the specified table is not disabled
   */
  void deleteTableIfExists(String namespace, String name) throws IOException;
}
