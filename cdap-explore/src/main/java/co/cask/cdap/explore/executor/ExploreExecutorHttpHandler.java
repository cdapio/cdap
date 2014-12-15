/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.hive.objectinspector.ObjectInspectorFactory;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements internal explore APIs.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/data/explore")
public class ExploreExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutorHttpHandler.class);

  private final ExploreService exploreService;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;

  @Inject
  public ExploreExecutorHttpHandler(ExploreService exploreService,
                                    DatasetFramework datasetFramework,
                                    StreamAdmin streamAdmin) {
    this.exploreService = exploreService;
    this.datasetFramework = datasetFramework;
    this.streamAdmin = streamAdmin;
  }

  @POST
  @Path("streams/{stream}/enable")
  public void enableStream(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                           @PathParam("stream") final String streamName) {
    try {

      String streamLocationURI;
      try {
        StreamConfig streamConfig = streamAdmin.getConfig(streamName);
        Location streamLocation = streamConfig.getLocation();
        if (streamLocation == null) {
          responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find location of stream " + streamName);
          return;
        }
        streamLocationURI = streamLocation.toURI().toString();
      } catch (IOException e) {
        LOG.info("Could not find stream {} to enable explore on.", streamName, e);
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find stream " + streamName);
        return;
      }

      LOG.debug("Enabling explore for stream {} at location {}", streamName, streamLocationURI);
      String createStatement;
      try {
        createStatement = generateStreamCreateStatement(streamName, streamLocationURI);
      } catch (UnsupportedTypeException e) {
        LOG.error("Exception while generating create statement for stream {}", streamName, e);
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      LOG.debug("Running create statement for stream {}", streamName);

      QueryHandle handle = exploreService.execute(createStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("streams/{stream}/disable")
  public void disableStream(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("stream") final String streamName) {
    try {
      LOG.debug("Disabling explore for stream {}", streamName);

      try {
        // throws io exception if there is no stream
        streamAdmin.getConfig(streamName);
      } catch (IOException e) {
        LOG.debug("Could not find stream {} to disable explore on.", streamName, e);
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find stream " + streamName);
        return;
      }

      String deleteStatement = generateDeleteStatement(getStreamTableName(streamName));
      LOG.debug("Running delete statement for stream {} - {}", streamName, deleteStatement);

      QueryHandle handle = exploreService.execute(deleteStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("/datasets/{dataset}/enable")
  public void enableDataset(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("dataset") final String datasetName) {
    try {
      Dataset dataset;
      try {
        dataset = datasetFramework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, null);
      } catch (Exception e) {
        String className = isClassNotFoundException(e);
        if (className == null) {
          throw e;
        }
        LOG.info("Cannot load dataset {} because class {} cannot be found. This is probably because class {} is a " +
                   "type parameter of dataset {} that is not present in the dataset's jar file. See the developer " +
                   "guide for more information.", datasetName, className, className, datasetName);
        JsonObject json = new JsonObject();
        json.addProperty("handle", QueryHandle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }
      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetName);
        return;
      }

      if (!(dataset instanceof RecordScannable || dataset instanceof RecordWritable)) {
        // It is not an error to get non-RecordEnabled datasets, since the type of dataset may not be known where this
        // call originates from.
        LOG.debug("Dataset {} neither implements {} nor {}", datasetName, RecordScannable.class.getName(),
                  RecordWritable.class.getName());
        JsonObject json = new JsonObject();
        json.addProperty("handle", QueryHandle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }

      LOG.debug("Enabling explore for dataset instance {}", datasetName);
      String createStatement;
      try {
        createStatement = generateCreateStatement(datasetName, dataset);
      } catch (UnsupportedTypeException e) {
        LOG.error("Exception while generating create statement for dataset {}", datasetName, e);
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      LOG.debug("Running create statement for dataset {} with row scannable {} - {}",
                datasetName,
                dataset.getClass().getName(),
                createStatement);

      QueryHandle handle = exploreService.execute(createStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private String isClassNotFoundException(Throwable e) {
    if (e instanceof ClassNotFoundException) {
      return e.getMessage();
    }
    if (e.getCause() != null) {
      return isClassNotFoundException(e.getCause());
    }
    return null;
  }

  /**
   * Disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("/datasets/{dataset}/disable")
  public void disableDataset(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                             @PathParam("dataset") final String datasetName) {
    try {
      LOG.debug("Disabling explore for dataset instance {}", datasetName);

      Dataset dataset = datasetFramework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, null);
      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetName);
        return;
      }

      if (!(dataset instanceof RecordScannable || dataset instanceof RecordWritable)) {
        // It is not an error to get non-Record enabled datasets, since the type of dataset may not be known where this
        // call originates from.
        LOG.debug("Dataset {} neither implements {} nor {}", datasetName, RecordScannable.class.getName(),
                  RecordWritable.class.getName());
        JsonObject json = new JsonObject();
        json.addProperty("handle", QueryHandle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }

      String deleteStatement = generateDeleteStatement(datasetName);
      LOG.debug("Running delete statement for dataset {} - {}", datasetName, deleteStatement);

      QueryHandle handle = exploreService.execute(deleteStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private static String getStreamTableName(String streamName) {
    return getHiveTableName("cdap_stream_" + streamName);
  }

  public static String getHiveTableName(String name) {
    // Instance name is like cdap.user.my_table.
    // For now replace . with _ since Hive tables cannot have . in them.
    return name.replaceAll("\\.", "_").toLowerCase();
  }

  /**
   * Generate the hive sql statement for creating a table to query the underlying stream. Note that Hive will put
   * in a dummy value for an external table if it is not given in the create statement, which will result in a
   * table that cannot be queried. As such, the location must be given and accurate.
   *
   * @param name name of the stream
   * @param location location of the stream
   * @return hive statement to use when creating the external table for querying the stream
   * @throws UnsupportedTypeException
   */
  public static String generateStreamCreateStatement(String name, String location) throws UnsupportedTypeException {
    String hiveSchema = hiveSchemaForStream();
    String tableName = getStreamTableName(name);
    return String.format("CREATE EXTERNAL TABLE %s %s COMMENT \"CDAP Stream\" " +
                           "STORED BY \"%s\" WITH SERDEPROPERTIES(\"%s\" = \"%s\") " +
                           "LOCATION \"%s\"",
                         tableName, hiveSchema, Constants.Explore.STREAM_STORAGE_HANDLER_CLASS,
                         Constants.Explore.STREAM_NAME, name, location);
  }

  public static String generateCreateStatement(String name, Dataset dataset)
    throws UnsupportedTypeException {
    String hiveSchema = hiveSchemaFor(dataset);
    String tableName = getHiveTableName(name);
    return String.format("CREATE EXTERNAL TABLE %s %s COMMENT \"CDAP Dataset\" " +
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

  static String hiveSchemaForStream() throws UnsupportedTypeException {
    return hiveSchemaFor(StreamSchema.class);
  }

  static String hiveSchemaFor(Type type) throws UnsupportedTypeException {
    // This call will make sure that the type is not recursive
    new ReflectionSchemaGenerator().generate(type, false);

    ObjectInspector objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(type);
    if (!(objectInspector instanceof StructObjectInspector)) {
      throw new UnsupportedTypeException(String.format("Type must be a RECORD, but is %s",
                                                       type.getClass().getName()));
    }
    StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;

    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (StructField structField : structObjectInspector.getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      ObjectInspector oi = structField.getFieldObjectInspector();
      String typeName;
      typeName = oi.getTypeName();
      sb.append(structField.getFieldName()).append(" ").append(typeName);
    }
    sb.append(")");

    return sb.toString();
  }

  // hardcoded schema for a stream. This will be replaced with a schema that comes from the stream when
  // schema is exposed to users.
  @SuppressWarnings("unused")
  private static class StreamSchema {
    // can't use 'timestamp' as it is a reserved Hive keyword (though not for all versions of Hive)
    private long ts;
    // the body is actually a byte array, but that is not very useful when performing queries.
    // we therefore assume the body is a string.
    private String body;
    private Map<String, String> headers;
  }
}
