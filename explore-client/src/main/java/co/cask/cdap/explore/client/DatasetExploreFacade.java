/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.service.UnexpectedQueryStatusException;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Explore client facade to be used by datasets.
 */
public class DatasetExploreFacade {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetExploreFacade.class);

  private final ExploreClient exploreClient;
  private final boolean exploreEnabled;

  @Inject
  public DatasetExploreFacade(ExploreClient exploreClient, CConfiguration cConf) {
    this.exploreClient = exploreClient;
    this.exploreEnabled = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED);
    if (!exploreEnabled) {
      LOG.warn("Explore functionality for datasets is disabled. All calls to enable explore will be no-ops");
    }
  }

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  public void enableExplore(String datasetInstance) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.enableExplore(datasetInstance);
    try {
      futureSuccess.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof ExploreException) {
        LOG.error("Enable explore did not finish successfully for dataset instance {}.",
                  datasetInstance);
        throw (ExploreException) t;
      } else if (t instanceof SQLException) {
        throw (SQLException) t;
      } else if (t instanceof HandleNotFoundException) {
        // Cannot happen unless explore server restarted, or someone calls close in between.
        LOG.error("Error running enable explore", e);
        throw Throwables.propagate(e);
      } else if (t instanceof UnexpectedQueryStatusException) {
        UnexpectedQueryStatusException sE = (UnexpectedQueryStatusException) t;
        LOG.error("Enable explore operation ended in an unexpected state - {}", sE.getStatus().name(), e);
        throw Throwables.propagate(e);
      }
    } catch (TimeoutException e) {
      LOG.error("Error running enable explore - operation timed out", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Disable ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   * @param datasetInstance dataset instance name.
   */
  public void disableExplore(String datasetInstance) throws ExploreException, SQLException {
    if (!exploreEnabled) {
      return;
    }

    ListenableFuture<Void> futureSuccess = exploreClient.disableExplore(datasetInstance);
    try {
      futureSuccess.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof ExploreException) {
        LOG.error("Disable explore did not finish successfully for dataset instance {}.",
                  datasetInstance);
        throw (ExploreException) t;
      } else if (t instanceof SQLException) {
        throw (SQLException) t;
      } else if (t instanceof HandleNotFoundException) {
        // Cannot happen unless explore server restarted, or someone calls close in between.
        LOG.error("Error running disable explore", e);
        throw Throwables.propagate(e);
      } else if (t instanceof UnexpectedQueryStatusException) {
        UnexpectedQueryStatusException sE = (UnexpectedQueryStatusException) t;
        LOG.error("Disable explore operation ended in an unexpected state - {}", sE.getStatus().name(), e);
        throw Throwables.propagate(e);
      }
    } catch (TimeoutException e) {
      LOG.error("Error running disable explore - operation timed out", e);
      throw Throwables.propagate(e);
    }
  }

  public static String getHiveTableName(String datasetName) {
    // TODO: fix namespacing - REACTOR-264
    // Instnace name is like continuuity.user.my_table.
    // For now replace . with _ since Hive tables cannot have . in them.
    return datasetName.replaceAll("\\.", "_").toLowerCase();
  }

  public static String generateCreateStatement(String name, Dataset dataset)
    throws UnsupportedTypeException {
    String hiveSchema = hiveSchemaFor(dataset);
    String tableName = getHiveTableName(name);
    return String.format("CREATE EXTERNAL TABLE %s %s COMMENT \"Cask CDAP Dataset\" " +
                           "STORED BY \"%s\" WITH SERDEPROPERTIES(\"%s\" = \"%s\")",
                         tableName, hiveSchema, Constants.Explore.DATASET_STORAGE_HANDLER_CLASS,
                         Constants.Explore.DATASET_NAME, name);
  }

  public static String generateDeleteStatement(String name) {
    return String.format("DROP TABLE IF EXISTS %s", getHiveTableName(name));
  }

  /**
   * Given a record-enabled dataset, determine its record type and generate a schema string compatible with Hive.
   * @param dataset The data set
   * @return the hive schema
   * @throws UnsupportedTypeException if the dataset is neither RecordScannable, nor RecordWritable,
   * or if the row type is not a record or contains null types.
   */
  static String hiveSchemaFor(Dataset dataset) throws UnsupportedTypeException {
    if (dataset instanceof RecordScannable) {
      return hiveSchemaFor(((RecordScannable) dataset).getRecordType());
    } else if (dataset instanceof RecordWritable) {
      return hiveSchemaFor(((RecordWritable) dataset).getRecordType());
    }
    throw new UnsupportedTypeException("Dataset neither implements RecordScannable not RecordWritable.");
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
