/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.spanner.Value;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.spi.data.StorageProviderContext;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableTest;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Unit tests for GCP spanner implementation of the {@link StructuredTable}. This test needs the following
 * Java properties to run. If they are not provided, tests will be ignored.
 *
 * <ul>
 *   <li>gcp.project - GCP project name</li>
 *   <li>gcp.spanner.instance - GCP spanner instance name</li>
 *   <li>gcp.spanner.database - GCP spanner database name</li>
 *   <li>(optional) gcp.credentials.path - Local file path to the service account
 *   json that has the "Cloud Spanner Database User" role</li>
 * </ul>
 */
@RunWith(JUnitParamsRunner.class)
public class SpannerStructuredTableTest extends StructuredTableTest {

  private static SpannerStorageProvider storageProvider;

  @BeforeClass
  public static void createSpannerStorageProvider() throws Exception {
    String project = System.getProperty("gcp.project");
    String instance = System.getProperty("gcp.spanner.instance");
    String database = System.getProperty("gcp.spanner.database");
    String credentialsPath = System.getProperty("gcp.credentials.path");

    // GCP project, instance, and database must be provided
    Assume.assumeNotNull(project, instance, database);

    Map<String, String> configs = new HashMap<>();
    configs.put(SpannerStorageProvider.PROJECT, project);
    configs.put(SpannerStorageProvider.INSTANCE, instance);
    configs.put(SpannerStorageProvider.DATABASE, database);
    configs.put(RetryingSpannerTransactionRunner.OPTIMISTIC_LOCK_ENABLED, "true");

    if (credentialsPath != null) {
      configs.put(SpannerStorageProvider.CREDENTIALS_PATH, credentialsPath);
    }

    StorageProviderContext context = new MockStorageProviderContext(configs);

    storageProvider = new SpannerStorageProvider();
    storageProvider.initialize(context);
  }

  @AfterClass
  public static void closeSpannerStorageProvider() {
    Optional.ofNullable(storageProvider).ifPresent(SpannerStorageProvider::close);
  }

  @Override
  @Ignore
  public void testSortedPrimaryKeyFilteredIndexScan() {
    // no implementation
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return storageProvider.getStructuredTableAdmin();
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return storageProvider.getTransactionRunner();
  }

  @Test
  @Parameters(method = "getCompositeKeyConditionScenarios")
  @TestCaseName("{0}")
  public void testGetCompositeKeyCondition(String description, List<Field<?>> fields,
      Range.Bound bound, boolean isLowerBound, int startParamIndex, String expectedSql,
      Map<String, Value> expectedParams) {
    SpannerStructuredTableSchema schema = new SpannerStructuredTableSchema(SIMPLE_SPEC.getTableId(),
        SIMPLE_SPEC.getFieldTypes(), SIMPLE_SPEC.getPrimaryKeys(), SIMPLE_SPEC.getIndexes(), null);
    try (SpannerStructuredTable table = new SpannerStructuredTable(null, schema, null)) {
      Map<String, Value> actualParams = new HashMap<>();
      String actualSql = table.getCompositeKeyCondition(fields, bound, isLowerBound, actualParams,
          startParamIndex);
      Assert.assertEquals("SQL mismatch in: " + description, expectedSql, actualSql);
      Assert.assertEquals("Params mismatch in: " + description, expectedParams, actualParams);
    }
  }

  private Object[] getCompositeKeyConditionScenarios() {
    return new Object[]{
        // Format: Name | Fields | Bound Type | isLower | startParamIndex | Expected SQL | Expected Params
        new Object[]{
            "COMPOSITE: (col1, col2) >= ('A', 5) [Lower Inclusive]",
            Arrays.asList(Fields.stringField("col1", "A"), Fields.intField("col2", 5)),
            Range.Bound.INCLUSIVE, true, 0,
            "((`col1` > @p_0) OR (`col1` = @p_0 AND `col2` >= @p_1))",
            params("p_0", Value.string("A"), "p_1", Value.int64(5))
        },
        new Object[]{
            "COMPOSITE: (col1, col2) > ('A', 5) [Lower Exclusive]",
            Arrays.asList(Fields.stringField("col1", "A"), Fields.intField("col2", 5)),
            Range.Bound.EXCLUSIVE, true, 0,
            "((`col1` > @p_0) OR (`col1` = @p_0 AND `col2` > @p_1))",
            params("p_0", Value.string("A"), "p_1", Value.int64(5))
        },
        new Object[]{
            "COMPOSITE: (col1, col2) <= ('B', 10) [Upper Inclusive]",
            Arrays.asList(Fields.stringField("col1", "B"), Fields.intField("col2", 10)),
            Range.Bound.INCLUSIVE, false, 0,
            "((`col1` < @p_0) OR (`col1` = @p_0 AND `col2` <= @p_1))",
            params("p_0", Value.string("B"), "p_1", Value.int64(10))
        },

        new Object[]{
            "SINGLE: col1 >= 'A' [Lower Inclusive]",
            Collections.singletonList(Fields.stringField("col1", "A")),
            Range.Bound.INCLUSIVE, true, 0,
            "((`col1` >= @p_0))",
            params("p_0", Value.string("A"))
        },
        new Object[]{
            "SINGLE: col1 < 'Z' [Upper Exclusive]",
            Collections.singletonList(Fields.stringField("col1", "Z")),
            Range.Bound.EXCLUSIVE, false, 0,
            "((`col1` < @p_0))",
            params("p_0", Value.string("Z"))
        },

        new Object[]{
            "DEEP: (k1, k2, k3, k4) <= ('x', 1, true, 99) [Upper Inclusive]",
            Arrays.asList(
                Fields.stringField("k1", "x"),
                Fields.intField("k2", 1),
                Fields.booleanField("k3", true),
                Fields.longField("k4", 99L)
            ),
            Range.Bound.INCLUSIVE, false, 0,
            "((`k1` < @p_0) OR " +
                "(`k1` = @p_0 AND `k2` < @p_1) OR " +
                "(`k1` = @p_0 AND `k2` = @p_1 AND `k3` < @p_2) OR " +
                "(`k1` = @p_0 AND `k2` = @p_1 AND `k3` = @p_2 AND `k4` <= @p_3))",
            params("p_0", Value.string("x"), "p_1", Value.int64(1), "p_2", Value.bool(true), "p_3",
                Value.int64(99L))
        },

        new Object[]{
            "EDGE: Empty field list returns empty string",
            Collections.emptyList(),
            Range.Bound.INCLUSIVE, true, 0,
            "",
            Collections.emptyMap()
        },

        new Object[]{
            "OFFSET: 1 Field, Lower Inclusive, startParamIndex = 5",
            Collections.singletonList(Fields.stringField("id", "123")),
            Range.Bound.INCLUSIVE, true, 5,
            "((`id` >= @p_5))",
            params("p_5", Value.string("123"))
        },
        new Object[]{
            "OFFSET: 2 Fields, Upper Exclusive, startParamIndex = 10",
            Arrays.asList(Fields.stringField("col1", "X"), Fields.intField("col2", 99)),
            Range.Bound.EXCLUSIVE, false, 10,
            "((`col1` < @p_10) OR (`col1` = @p_10 AND `col2` < @p_11))",
            params("p_10", Value.string("X"), "p_11", Value.int64(99))
        }
    };
  }

  private static Map<String, Value> params(Object... entries) {
    Map<String, Value> map = new HashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      map.put((String) entries[i], (Value) entries[i + 1]);
    }
    return map;
  }

  private static final class MockStorageProviderContext implements StorageProviderContext {

    private final Map<String, String> config;

    MockStorageProviderContext(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public MetricsCollector getMetricsCollector() {
      return new MetricsCollector() {
        @Override
        public void increment(String metricName, long value) {
          // no-op
        }

        @Override
        public void gauge(String metricName, long value) {
          // no-op
        }
      };
    }

    @Override
    public Map<String, String> getConfiguration() {
      return config;
    }

    @Override
    public Map<String, String> getSecurityConfiguration() {
      return Collections.emptyMap();
    }
  }
}
