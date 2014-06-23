package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.client.InternalAsyncExploreClient;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.explore.service.hive.Hive13ExploreService;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HiveMetadataTest {
  private static Injector injector;
  private static InMemoryTransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;
  private static ExploreExecutorService exploreExecutorService;
  private static ExploreClient exploreClient;

  @BeforeClass
  public static void start() throws Exception {
    injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    KeyStructValueTableDefinition.KeyValue.Value value1 = new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 = new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));

    exploreClient = injector.getInstance(InternalAsyncExploreClient.class);
    Assert.assertTrue(exploreClient.isAvailable());

  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("keyStructValue");

    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Test
  public void testGetColumns() throws Exception {
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = new RandomEndpointStrategy(
        discoveryServiceClient.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();
    InetSocketAddress addr = discoverable.getSocketAddress();

    Hive13ExploreService exploreService = (Hive13ExploreService) injector.getInstance(ExploreService.class);
    Handle handle = exploreService.getColumns(null, null, ".*", ".*");
    // Kind of results: Next
//    [
//    {
//      "columns": [
//      null,
//          "default",
//          "continuuity_user_my_table",
//          "key",
//          12,
//          "STRING",
//          2147483647,
//          null,
//          null,
//          null,
//          1,
//          "from deserializer",
//          null,
//          null,
//          null,
//          null,
//          1,
//          "YES",
//          null,
//          null,
//          null,
//          null,
//          "NO"
//      ]
//    },
//    {
//      "columns": [
//      null,
//          "default",
//          "continuuity_user_my_table",
//          "value",
//          2002,
//          "struct<name:string,ints:array<int>>",
//          null,
//          null,
//          null,
//          null,
//          1,
//          "from deserializer",
//          null,
//          null,
//          null,
//          null,
//          2,
//          "YES",
//          null,
//          null,
//          null,
//          null,
//          "NO"
//      ]
//    }
//    ]
    // Schema:
//    [
//    {
//      "comment": "Catalog name. NULL if not applicable",
//        "name": "TABLE_CAT",
//        "position": 1,
//        "type": "STRING"
//    },
//    {
//      "comment": "Schema name",
//        "name": "TABLE_SCHEM",
//        "position": 2,
//        "type": "STRING"
//    },
//    {
//      "comment": "Table name",
//        "name": "TABLE_NAME",
//        "position": 3,
//        "type": "STRING"
//    },
//    {
//      "comment": "Column name",
//        "name": "COLUMN_NAME",
//        "position": 4,
//        "type": "STRING"
//    },
//    {
//      "comment": "SQL type from java.sql.Types",
//        "name": "DATA_TYPE",
//        "position": 5,
//        "type": "INT"
//    },
//    {
//      "comment": "Data source dependent type name, for a UDT the type name is fully qualified",
//        "name": "TYPE_NAME",
//        "position": 6,
//        "type": "STRING"
//    },
//    {
//      "comment": "Column size. For char or date types this is the maximum number of characters, for numeric or decimal types this is precision.",
//        "name": "COLUMN_SIZE",
//        "position": 7,
//        "type": "INT"
//    },
//    {
//      "comment": "Unused",
//        "name": "BUFFER_LENGTH",
//        "position": 8,
//        "type": "TINYINT"
//    },
//    {
//      "comment": "The number of fractional digits",
//        "name": "DECIMAL_DIGITS",
//        "position": 9,
//        "type": "INT"
//    },
//    {
//      "comment": "Radix (typically either 10 or 2)",
//        "name": "NUM_PREC_RADIX",
//        "position": 10,
//        "type": "INT"
//    },
//    {
//      "comment": "Is NULL allowed",
//        "name": "NULLABLE",
//        "position": 11,
//        "type": "INT"
//    },
//    {
//      "comment": "Comment describing column (may be null)",
//        "name": "REMARKS",
//        "position": 12,
//        "type": "STRING"
//    },
//    {
//      "comment": "Default value (may be null)",
//        "name": "COLUMN_DEF",
//        "position": 13,
//        "type": "STRING"
//    },
//    {
//      "comment": "Unused",
//        "name": "SQL_DATA_TYPE",
//        "position": 14,
//        "type": "INT"
//    },
//    {
//      "comment": "Unused",
//        "name": "SQL_DATETIME_SUB",
//        "position": 15,
//        "type": "INT"
//    },
//    {
//      "comment": "For char types the maximum number of bytes in the column",
//        "name": "CHAR_OCTET_LENGTH",
//        "position": 16,
//        "type": "INT"
//    },
//    {
//      "comment": "Index of column in table (starting at 1)",
//        "name": "ORDINAL_POSITION",
//        "position": 17,
//        "type": "INT"
//    },
//    {
//      "comment": "\"NO\" means column definitely does not allow NULL values; \"YES\" means the column might allow NULL values. An empty string means nobody knows.",
//        "name": "IS_NULLABLE",
//        "position": 18,
//        "type": "STRING"
//    },
//    {
//      "comment": "Catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)",
//        "name": "SCOPE_CATALOG",
//        "position": 19,
//        "type": "STRING"
//    },
//    {
//      "comment": "Schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)",
//        "name": "SCOPE_SCHEMA",
//        "position": 20,
//        "type": "STRING"
//    },
//    {
//      "comment": "Table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)",
//        "name": "SCOPE_TABLE",
//        "position": 21,
//        "type": "STRING"
//    },
//    {
//      "comment": "Source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)",
//        "name": "SOURCE_DATA_TYPE",
//        "position": 22,
//        "type": "SMALLINT"
//    },
//    {
//      "comment": "Indicates whether this column is auto incremented.",
//        "name": "IS_AUTO_INCREMENT",
//        "position": 23,
//        "type": "STRING"
//    }
//    ]


    handle = exploreService.getTables(null, null, ".*", null);
    // Kind of results: Next
//    [
//    {
//      "columns": [
//      "",
//          "default",
//          "continuuity_user_my_table",
//          "TABLE",
//          "Continuuity Reactor Dataset"
//      ]
//    }
//    ]
    // Schema:
//    [
//    {
//      "comment": "Catalog name. NULL if not applicable.",
//        "name": "TABLE_CAT",
//        "position": 1,
//        "type": "STRING"
//    },
//    {
//      "comment": "Schema name.",
//        "name": "TABLE_SCHEM",
//        "position": 2,
//        "type": "STRING"
//    },
//    {
//      "comment": "Table name.",
//        "name": "TABLE_NAME",
//        "position": 3,
//        "type": "STRING"
//    },
//    {
//      "comment": "The table type, e.g. \"TABLE\", \"VIEW\", etc.",
//        "name": "TABLE_TYPE",
//        "position": 4,
//        "type": "STRING"
//    },
//    {
//      "comment": "Comments about the table.",
//        "name": "REMARKS",
//        "position": 5,
//        "type": "STRING"
//    }
//    ]
//    [


    handle = exploreService.getTypeInfo();
    // Schema:
//    [
//    {
//      "comment": "Type name",
//        "name": "TYPE_NAME",
//        "position": 1,
//        "type": "STRING"
//    },
//    {
//      "comment": "SQL data type from java.sql.Types",
//        "name": "DATA_TYPE",
//        "position": 2,
//        "type": "INT"
//    },
//    {
//      "comment": "Maximum precision",
//        "name": "PRECISION",
//        "position": 3,
//        "type": "INT"
//    },
//    {
//      "comment": "Prefix used to quote a literal (may be null)",
//        "name": "LITERAL_PREFIX",
//        "position": 4,
//        "type": "STRING"
//    },
//    {
//      "comment": "Suffix used to quote a literal (may be null)",
//        "name": "LITERAL_SUFFIX",
//        "position": 5,
//        "type": "STRING"
//    },
//    {
//      "comment": "Parameters used in creating the type (may be null)",
//        "name": "CREATE_PARAMS",
//        "position": 6,
//        "type": "STRING"
//    },
//    {
//      "comment": "Can you use NULL for this type",
//        "name": "NULLABLE",
//        "position": 7,
//        "type": "SMALLINT"
//    },
//    {
//      "comment": "Is it case sensitive",
//        "name": "CASE_SENSITIVE",
//        "position": 8,
//        "type": "BOOLEAN"
//    },
//    {
//      "comment": "Can you use \"WHERE\" based on this type",
//        "name": "SEARCHABLE",
//        "position": 9,
//        "type": "SMALLINT"
//    },
//    {
//      "comment": "Is it unsigned",
//        "name": "UNSIGNED_ATTRIBUTE",
//        "position": 10,
//        "type": "BOOLEAN"
//    },
//    {
//      "comment": "Can it be a money value",
//        "name": "FIXED_PREC_SCALE",
//        "position": 11,
//        "type": "BOOLEAN"
//    },
//    {
//      "comment": "Can it be used for an auto-increment value",
//        "name": "AUTO_INCREMENT",
//        "position": 12,
//        "type": "BOOLEAN"
//    },
//    {
//      "comment": "Localized version of type name (may be null)",
//        "name": "LOCAL_TYPE_NAME",
//        "position": 13,
//        "type": "STRING"
//    },
//    {
//      "comment": "Minimum scale supported",
//        "name": "MINIMUM_SCALE",
//        "position": 14,
//        "type": "SMALLINT"
//    },
//    {
//      "comment": "Maximum scale supported",
//        "name": "MAXIMUM_SCALE",
//        "position": 15,
//        "type": "SMALLINT"
//    },
//    {
//      "comment": "Unused",
//        "name": "SQL_DATA_TYPE",
//        "position": 16,
//        "type": "INT"
//    },
//    {
//      "comment": "Unused",
//        "name": "SQL_DATETIME_SUB",
//        "position": 17,
//        "type": "INT"
//    },
//    {
//      "comment":"Usually 2 or 10",
//        "name":"NUM_PREC_RADIX",
//        "position":18,
//        "type":"INT"
//    }
//    ]
      // Next:
//    [
//    {
//      "columns": [
//      "VOID",
//          0,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "BOOLEAN",
//          16,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "TINYINT",
//          -6,
//          3,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          10
//      ]
//    },
//    {
//      "columns": [
//      "SMALLINT",
//          5,
//          5,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          10
//      ]
//    },
//    {
//      "columns": [
//      "INT",
//          4,
//          10,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          10
//      ]
//    },
//    {
//      "columns": [
//      "BIGINT",
//          -5,
//          19,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          10
//      ]
//    },
//    {
//      "columns": [
//      "FLOAT",
//          6,
//          7,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          2
//      ]
//    },
//    {
//      "columns": [
//      "DOUBLE",
//          8,
//          15,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          2
//      ]
//    },
//    {
//      "columns": [
//      "STRING",
//          12,
//          null,
//          null,
//          null,
//          null,
//          1,
//          true,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "CHAR",
//          1,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "VARCHAR",
//          12,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "DATE",
//          91,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "TIMESTAMP",
//          93,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "BINARY",
//          -2,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "DECIMAL",
//          3,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          3,
//          false,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "ARRAY",
//          2003,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          0,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "MAP",
//          2000,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          0,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "STRUCT",
//          2002,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          0,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "UNIONTYPE",
//          1111,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          0,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    },
//    {
//      "columns": [
//      "USER_DEFINED",
//          1111,
//          null,
//          null,
//          null,
//          null,
//          1,
//          false,
//          0,
//          true,
//          false,
//          false,
//          null,
//          0,
//          0,
//          null,
//          null,
//          null
//      ]
//    }
//    ]

    }

  private static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs, List<Result> expectedResults) throws Exception {
    Handle handle = exploreClient.execute(command);

    Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 20);
    Assert.assertEquals(Status.OpStatus.FINISHED, status.getStatus());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, exploreClient.getResultSchema(handle));
    Assert.assertEquals(expectedResults, trimColumnValues(exploreClient.nextResults(handle, 100)));

    exploreClient.close(handle);
  }

  private static List<Result> trimColumnValues(List<Result> results) {
    List<Result> newResults = Lists.newArrayList();
    for (Result result : results) {
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : result.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else {
          newCols.add(obj);
        }
      }
      newResults.add(new Result(newCols));
    }
    return newResults;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.CFG_EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
        new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
        new ConfigModule(configuration, hConf),
        new IOModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DataSetServiceModules().getInMemoryModule(),
        new DataFabricModules().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new AuthModule(),
        new ExploreRuntimeModule().getInMemoryModules()
    );
  }
}
