/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.client;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;
import com.google.common.base.Objects;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods for explore client.
 */
public class ExploreClientUtil {

  /**
   * Polls for state of the operation represented by the {@link Handle}, and returns when operation has completed
   * execution.
   * @param exploreClient explore client used to poll status.
   * @param handle handle representing the operation.
   * @param sleepTime time to sleep between pooling.
   * @param timeUnit unit of sleepTime.
   * @param maxTries max number of times to poll.
   * @return completion status of the operation, null on reaching maxTries.
   * @throws ExploreException
   * @throws HandleNotFoundException
   * @throws InterruptedException
   */
  public static Status waitForCompletionStatus(Explore exploreClient, Handle handle,
                                               long sleepTime, TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException, SQLException {
    Status status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreClient.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (status.getStatus() == Status.OpStatus.RUNNING || status.getStatus() == Status.OpStatus.PENDING ||
             status.getStatus() == Status.OpStatus.INITIALIZED || status.getStatus() == Status.OpStatus.UNKNOWN);
    return status;
  }

  /**
   * Class to represent a JSON object passed as an argument to the getTables metadata HTTP endpoint.
   */
  public static final class TablesArgs {
    private final String catalog;
    private final String schemaPattern;
    private final String tableNamePattern;
    private final List<String> tableTypes;

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("catalog", catalog)
        .add("schemaPattern", schemaPattern)
        .add("tableNamePattern", tableNamePattern)
        .add("tableTypes", tableTypes)
        .toString();
    }

    public TablesArgs(String catalog, String schemaNamePattern, String tableNamePattern, List<String> tableTypes) {
      this.catalog = catalog;
      this.schemaPattern = schemaNamePattern;
      this.tableNamePattern = tableNamePattern;
      this.tableTypes = tableTypes;
    }

    public String getTableNamePattern() {
      return tableNamePattern;
    }

    public List<String> getTableTypes() {
      return tableTypes;
    }

    public String getSchemaPattern() {
      return schemaPattern;
    }

    public String getCatalog() {
      return catalog;
    }
  }

  /**
   * Class to represent a JSON object passed as an argument to the getColumns metadata HTTP endpoint.
   */
  public static final class ColumnsArgs {
    private final String catalog;
    private final String schemaPattern;
    private final String tableNamePattern;
    private final String columnNamePattern;

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("catalog", catalog)
        .add("schemaPattern", schemaPattern)
        .add("tableNamePattern", tableNamePattern)
        .add("columnNamePattern", columnNamePattern)
        .toString();
    }

    public ColumnsArgs(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.tableNamePattern = tableNamePattern;
      this.columnNamePattern = columnNamePattern;
    }

    public String getTableNamePattern() {
      return tableNamePattern;
    }

    public String getColumnNamePattern() {
      return columnNamePattern;
    }

    public String getSchemaPattern() {
      return schemaPattern;
    }

    public String getCatalog() {
      return catalog;
    }
  }

  /**
   * Class to represent a JSON object passed as an argument to the getSchemas metadata HTTP endpoint.
   */
  public static final class SchemaArgs {
    private final String catalog;
    private final String schemaPattern;

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("catalog", catalog)
        .add("schemaPattern", schemaPattern)
        .toString();
    }

    public SchemaArgs(String catalog, String schemaPattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
    }

    public String getSchemaPattern() {
      return schemaPattern;
    }

    public String getCatalog() {
      return catalog;
    }
  }

  /**
   * Class to represent a JSON object passed as an argument to the getFunctions metadata HTTP endpoint.
   */
  public static final class FunctionsArgs {
    private final String catalog;
    private final String schemaPattern;
    private final String functionNamePattern;

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("catalog", catalog)
        .add("schemaPattern", schemaPattern)
        .add("functionNamePattern", functionNamePattern)
        .toString();
    }

    public FunctionsArgs(String catalog, String schemaPattern, String functionNamePattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.functionNamePattern = functionNamePattern;
    }

    public String getFunctionNamePattern() {
      return functionNamePattern;
    }

    public String getSchemaPattern() {
      return schemaPattern;
    }

    public String getCatalog() {
      return catalog;
    }
  }
}
