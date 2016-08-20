/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.EnableExploreParameters;
import co.cask.cdap.explore.client.UpdateExploreParameters;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements internal explore APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/data/explore")
public class ExploreExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreExecutorHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final ExploreTableManager exploreTableManager;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;

  @Inject
  public ExploreExecutorHttpHandler(ExploreTableManager exploreTableManager,
                                    DatasetFramework datasetFramework,
                                    StreamAdmin streamAdmin,
                                    SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    this.exploreTableManager = exploreTableManager;
    this.datasetFramework = datasetFramework;
    this.streamAdmin = streamAdmin;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
  }

  @POST
  @Path("streams/{stream}/tables/{table}/enable")
  public void enableStream(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespace,
                           @PathParam("stream") String streamName,
                           @PathParam("table") String tableName) throws Exception {
    StreamId streamId = new StreamId(namespace, streamName);
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()))) {
      FormatSpecification format = GSON.fromJson(reader, FormatSpecification.class);
      if (format == null) {
        throw new BadRequestException("Expected format in the body");
      }
      QueryHandle handle = exploreTableManager.enableStream(tableName, streamId, format);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (UnsupportedTypeException e) {
      LOG.error("Exception while generating create statement for stream {}", streamName, e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  @POST
  @Path("streams/{stream}/tables/{table}/disable")
  public void disableStream(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("stream") String streamName,
                            @PathParam("table") String tableName) {

    StreamId streamId = new StreamId(namespace, streamName);
    try {
      // throws io exception if there is no stream
      streamAdmin.getConfig(streamId.toId());
    } catch (IOException e) {
      LOG.debug("Could not find stream {} to disable explore on.", streamName, e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find stream " + streamName);
      return;
    }

    try {
      QueryHandle handle = exploreTableManager.disableStream(tableName, streamId);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable t) {
      LOG.error("Got exception disabling exploration for stream {}", streamId, t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @POST
  @Path("datasets/{dataset}/enable-internal")
  public void enableInternal(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace,
                             @PathParam("dataset") String datasetName)
    throws BadRequestException, IOException {

    enableDataset(responder, new DatasetId(namespace, datasetName), readEnableParameters(request).getSpec());
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/enable")
  public void enableDataset(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName) {
    DatasetId datasetId = new DatasetId(namespace, datasetName);
    DatasetSpecification datasetSpec;
    try {
      datasetSpec = datasetFramework.getDatasetSpec(datasetId.toId());
      if (datasetSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetId);
        return;
      }
    } catch (DatasetManagementException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error getting spec for dataset " + datasetId);
      return;
    }
    enableDataset(responder, datasetId, datasetSpec);
  }

  private void enableDataset(HttpResponder responder, DatasetId datasetId, DatasetSpecification datasetSpec) {
    try {
      QueryHandle handle = exploreTableManager.enableDataset(datasetId, datasetSpec);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (ExploreException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error enabling explore on dataset " + datasetId);
    } catch (SQLException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "SQL exception while trying to enable explore on dataset " + datasetId);
    } catch (UnsupportedTypeException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Schema for dataset " + datasetId + " is not supported for exploration: " + e.getMessage());
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/update")
  public void updateDataset(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName)
    throws BadRequestException {

    DatasetId datasetId = new DatasetId(namespace, datasetName);
    try {
      UpdateExploreParameters params = readUpdateParameters(request);
      DatasetSpecification oldSpec = params.getOldSpec();
      DatasetSpecification datasetSpec = params.getNewSpec();

      QueryHandle handle;
      if (oldSpec.equals(datasetSpec)) {
        handle = QueryHandle.NO_OP;
      } else {
        handle = exploreTableManager.updateDataset(datasetId, datasetSpec, oldSpec);
      }
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (ExploreException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error updating explore on dataset " + datasetId);
    } catch (SQLException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "SQL exception while trying to update explore on dataset " + datasetId);
    } catch (UnsupportedTypeException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Schema for dataset " + datasetId + " is not supported for exploration: " + e.getMessage());
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private static EnableExploreParameters readEnableParameters(HttpRequest request)
    throws BadRequestException, IOException {
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      return GSON.fromJson(reader, EnableExploreParameters.class);
    } catch (JsonSyntaxException | NullPointerException e) {
      throw new BadRequestException("Cannot read dataset specification from request: " + e.getMessage(), e);
    }
  }

  private static UpdateExploreParameters readUpdateParameters(HttpRequest request)
    throws BadRequestException, IOException {
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      return GSON.fromJson(reader, UpdateExploreParameters.class);
    } catch (JsonSyntaxException | NullPointerException e) {
      throw new BadRequestException("Cannot read dataset specification from request: " + e.getMessage(), e);
    }
  }

  /**
   * Disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/disable")
  public void disableDataset(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName) {

    LOG.debug("Disabling explore for dataset instance {}", datasetName);
    DatasetId datasetId = new DatasetId(namespace, datasetName);
    try {
      QueryHandle handle = exploreTableManager.disableDataset(datasetId);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception while trying to disable explore on dataset {}", datasetId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("datasets/{dataset}/partitions")
  public void addPartition(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName) {
    DatasetId datasetId = new DatasetId(namespace, datasetName);
    propagateUserId(request);
    Dataset dataset;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      dataset = datasetInstantiator.getDataset(datasetId.toId());

      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetId);
        return;
      }
    } catch (IOException e) {
      String classNotFoundMessage = isClassNotFoundException(e);
      if (classNotFoundMessage != null) {
        JsonObject json = new JsonObject();
        json.addProperty("handle", QueryHandle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }
      LOG.error("Exception instantiating dataset {}.", datasetId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Exception instantiating dataset " + datasetName);
      return;
    }

    try {
      if (!(dataset instanceof PartitionedFileSet)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "not a partitioned dataset.");
        return;
      }
      Partitioning partitioning = ((PartitionedFileSet) dataset).getPartitioning();

      Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
      Map<String, String> properties = GSON.fromJson(reader, new TypeToken<Map<String, String>>() { }.getType());
      String fsPath = properties.get("path");
      if (fsPath == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "path was not specified.");
        return;
      }

      PartitionKey partitionKey;
      try {
        partitionKey = PartitionedFileSetArguments.getOutputPartitionKey(properties, partitioning);
      } catch (Exception e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "invalid partition key: " + e.getMessage());
        return;
      }
      if (partitionKey == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "no partition key was given.");
        return;
      }

      QueryHandle handle = exploreTableManager.addPartition(datasetId, partitionKey, fsPath);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  // this should really be a DELETE request. However, the partition key must be passed in the body
  // of the request, and that does not work with many HTTP clients, including Java's URLConnection.
  @POST
  @Path("datasets/{dataset}/deletePartition")
  public void dropPartition(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dataset") String datasetName) {
    DatasetId datasetId = new DatasetId(namespace, datasetName);
    propagateUserId(request);
    Dataset dataset;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      dataset = datasetInstantiator.getDataset(datasetId.toId());
      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetId);
        return;
      }
    } catch (IOException e) {
      String classNotFoundMessage = isClassNotFoundException(e);
      if (classNotFoundMessage != null) {
        JsonObject json = new JsonObject();
        json.addProperty("handle", QueryHandle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }
      LOG.error("Exception instantiating dataset {}.", datasetId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Exception instantiating dataset " + datasetId);
      return;
    }

    try {
      if (!(dataset instanceof PartitionedFileSet)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "not a partitioned dataset.");
        return;
      }
      Partitioning partitioning = ((PartitionedFileSet) dataset).getPartitioning();

      Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
      Map<String, String> properties = GSON.fromJson(reader, new TypeToken<Map<String, String>>() { }.getType());

      PartitionKey partitionKey;
      try {
        partitionKey = PartitionedFileSetArguments.getOutputPartitionKey(properties, partitioning);
      } catch (Exception e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "invalid partition key: " + e.getMessage());
        return;
      }
      if (partitionKey == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "no partition key was given.");
        return;
      }

      QueryHandle handle = exploreTableManager.dropPartition(datasetId, partitionKey);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  // returns the cause of the class not found exception if it is one. Otherwise returns null.
  @Nullable
  private static String isClassNotFoundException(Throwable e) {
    if (e instanceof ClassNotFoundException) {
      return e.getMessage();
    }
    if (e.getCause() != null) {
      return isClassNotFoundException(e.getCause());
    }
    return null;
  }

  // propagate userid from the HTTP Request in the current thread
  private void propagateUserId(HttpRequest request) {
    String userId = request.getHeader(Constants.Security.Headers.USER_ID);
    if (userId != null) {
      LOG.debug("Propagating userId as {}", userId);
      SecurityRequestContext.setUserId(userId);
    }
  }
}
