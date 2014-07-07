package com.continuuity.explore.client;

import com.continuuity.api.data.batch.RecordScannable;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Explore client facade to be used by datasets.
 */
public class DatasetExploreFacade {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetExploreFacade.class);

  private final ExploreClient exploreClient;
  private final boolean exploreEnabled;

  @Inject
  public DatasetExploreFacade(DiscoveryExploreClient exploreClient, CConfiguration cConf) {
    this.exploreClient = exploreClient;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.CFG_EXPLORE_ENABLED);
    if (!exploreEnabled) {
      LOG.warn("Explore functionality for datasets is disabled. All calls to enable explore will be no-ops");
    }
  }

  /**
   * Enables ad-hoc exploration of the given {@link com.continuuity.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  public void enableExplore(String datasetInstance) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    Handle handle = exploreClient.enableExplore(datasetInstance);
    try {
      Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 50);

      if (status.getStatus() != Status.OpStatus.FINISHED) {
        LOG.error("Enable explore did not finish successfully for dataset instance {}. Got final state - {}",
                  datasetInstance, status.getStatus());
        throw new ExploreException("Cannot enable explore for dataset instance " + datasetInstance);
      }
    } catch (HandleNotFoundException e) {
      // Cannot happen unless explore server restarted.
      LOG.error("Error running enable explore", e);
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
    } finally {
      try {
        exploreClient.close(handle);
      } catch (HandleNotFoundException e) {
        LOG.error("Ignoring cannot find handle during close of enable explore for dataset instance {}",
                  datasetInstance);
      }
    }
  }

  /**
   * Disable ad-hoc exploration of the given {@link com.continuuity.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  public void disableExplore(String datasetInstance) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    Handle handle = exploreClient.disableExplore(datasetInstance);
    try {
      Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 50);

      if (status.getStatus() != Status.OpStatus.FINISHED) {
        LOG.error("Disable explore did not finish successfully for dataset instance {}. Got final state - {}",
                  datasetInstance, status.getStatus());
        throw new ExploreException("Cannot disable explore for dataset instance " + datasetInstance);
      }
    } catch (HandleNotFoundException e) {
      // Cannot happen unless explore server restarted.
      LOG.error("Error running disable explore", e);
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
    } finally {
      try {
        exploreClient.close(handle);
      } catch (HandleNotFoundException e) {
        LOG.error("Ignoring cannot find handle during close of disable explore for dataset instance {}",
                  datasetInstance);
      }
    }
  }

  public static <ROW> String generateCreateStatement(String name, RecordScannable<ROW> scannable)
    throws UnsupportedTypeException {
    String hiveSchema = hiveSchemaFor(scannable);

    // TODO: fix namespacing - REACTOR-264
    // Instnace name is like continuuity.user.my_table.
    // For now replace . with _ since Hive tables cannot have . in them.
    String tableName = name.replaceAll("\\.", "_");

    return String.format("CREATE EXTERNAL TABLE %s %s COMMENT \"Continuuity Reactor Dataset\" " +
                           "STORED BY \"%s\" WITH SERDEPROPERTIES(\"%s\" = \"%s\")",
                         tableName, hiveSchema, Constants.Explore.DATASET_STORAGE_HANDLER_CLASS,
                         Constants.Explore.DATASET_NAME, name);
  }

  public static String generateDeleteStatement(String name) {
    // TODO: fix namespacing - REACTOR-264
    // Instnace name is like continuuity.user.my_table.
    // For now replace . with _ since Hive tables cannot have . in them.
    String tableName = name.replaceAll("\\.", "_");

    return String.format("DROP TABLE IF EXISTS %s", tableName);
  }

  /**
   * Given a row-scannable dataset, determine its row type and generate a schema string compatible with Hive.
   * @param dataset The data set
   * @param <ROW> The row type
   * @return the hive schema
   * @throws UnsupportedTypeException if the row type is not a record or contains null types.
   */
  static <ROW> String hiveSchemaFor(RecordScannable<ROW> dataset) throws UnsupportedTypeException {
    return hiveSchemaFor(dataset.getRecordType());
  }

  static String hiveSchemaFor(Type type) throws UnsupportedTypeException {

    Schema schema = new ReflectionSchemaGenerator().generate(type, false); // disallow recursive type
    if (!Schema.Type.RECORD.equals(schema.getType())) {
      throw new UnsupportedTypeException("type must be a RECORD but is " + schema.getType().name());
    }
    StringWriter writer = new StringWriter();
    writer.append('(');
    generateRecordSchema(schema, writer, " ");
    writer.append(')');
    return writer.toString();
  }

  // TODO: Add more test cases for different schema types. - REACTOR-266
  private static void generateHiveSchema(Schema schema, StringWriter writer) throws UnsupportedTypeException {

    switch (schema.getType()) {

      case NULL:
        throw new UnsupportedTypeException("Null schema not supported.");
      case BOOLEAN:
        writer.append("BOOLEAN");
        break;
      case INT:
        writer.append("INT");
        break;
      case LONG:
        writer.append("BIGINT");
        break;
      case FLOAT:
        writer.append("FLOAT");
        break;
      case DOUBLE:
        writer.append("DOUBLE");
        break;
      case BYTES:
        writer.append("BINARY");
        break;
      case STRING:
        writer.append("STRING");
        break;
      case ENUM:
        writer.append("STRING");
        break;
      case ARRAY:
        writer.append("ARRAY<");
        generateHiveSchema(schema.getComponentSchema(), writer);
        writer.append('>');
        break;
      case MAP:
        writer.append("MAP<");
        generateHiveSchema(schema.getMapSchema().getKey(), writer);
        writer.append(',');
        generateHiveSchema(schema.getMapSchema().getValue(), writer);
        writer.append('>');
        break;
      case RECORD:
        writer.append("STRUCT<");
        generateRecordSchema(schema, writer, ":");
        writer.append('>');
        break;
      case UNION:
        List<Schema> subSchemas = schema.getUnionSchemas();
        if (subSchemas.size() == 2 && Schema.Type.NULL.equals(subSchemas.get(1).getType())) {
          generateHiveSchema(subSchemas.get(0), writer);
          break;
        }
        writer.append("UNIONTYPE<");
        boolean first = true;
        for (Schema subSchema : schema.getUnionSchemas()) {
          if (!first) {
            writer.append(", ");
          } else {
            first = false;
          }
          generateHiveSchema(subSchema, writer);
        }
        writer.append(">");
        break;
    }

  }

  private static void generateRecordSchema(Schema schema, StringWriter writer, String separator)
    throws UnsupportedTypeException {
    boolean first = true;
    for (Schema.Field field : schema.getFields()) {
      if (!first) {
        writer.append(", ");
      } else {
        first = false;
      }
      writer.append(field.getName());
      writer.append(separator);
      generateHiveSchema(field.getSchema(), writer);
    }
  }
}
