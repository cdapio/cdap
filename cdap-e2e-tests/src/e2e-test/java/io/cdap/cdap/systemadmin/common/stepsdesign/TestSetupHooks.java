/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.cdap.systemadmin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import java.io.IOException;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

/**
 * GCP test hooks.
 */
public class TestSetupHooks {

  public static String bqTargetTable = StringUtils.EMPTY;
  public static String bqSourceTable = StringUtils.EMPTY;
  public static String datasetName = PluginPropertyUtils.pluginProp("dataset");

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    bqTargetTable = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTable);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTable);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    try {
      BigQueryClient.dropBqQuery(bqTargetTable);
      PluginPropertyUtils.removePluginProp("bqTargetTable");
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTable + " deleted successfully");
      bqTargetTable = StringUtils.EMPTY;
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTable + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table with 3 columns (Id - Int, Value - Int, UID - string) containing random testdata.
   * Sample row:
   * Id | Value | UID
   * 22 | 968   | 245308db-6088-4db2-a933-f0eea650846a
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    StringBuilder records = new StringBuilder(StringUtils.EMPTY);
    for (int index = 2; index <= 25; index++) {
      records.append(" (").append(index).append(", ").append((int) (Math.random() * 1000 + 1)).append(", '")
        .append(UUID.randomUUID()).append("'), ");
    }
    BigQueryClient.getSoleQueryResult("create table `" + datasetName + "." + bqSourceTable + "` as " +
                                        "SELECT * FROM UNNEST([ " +
                                        " STRUCT(1 AS Id, " + ((int) (Math.random() * 1000 + 1)) + " as Value, " +
                                        "'" + UUID.randomUUID() + "' as UID), " +
                                        records +
                                        "  (26, " + ((int) (Math.random() * 1000 + 1)) + ", " +
                                        "'" + UUID.randomUUID() + "') " +
                                        "])");
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    BigQueryClient.dropBqQuery(bqSourceTable);
    PluginPropertyUtils.removePluginProp("bqSourceTable");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    bqSourceTable = StringUtils.EMPTY;
  }
}
