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

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreTableManager;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements internal explore APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/data/explore")
public class ExploreExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreExecutorHttpHandler.class);
  private static final Gson GSON = new Gson();

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
  @Path("streams/{stream}/enable")
  public void enableStream(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId, @PathParam("stream") String streamName) {
    Id.Stream streamId = Id.Stream.from(namespaceId, streamName);
    StreamConfig streamConfig;
    try {
      streamConfig = streamAdmin.getConfig(streamId);
      Location streamLocation = streamConfig.getLocation();
      if (streamLocation == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find location of stream " + streamName);
        return;
      }
    } catch (IOException e) {
      LOG.info("Could not find stream {} to enable explore on.", streamName, e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find stream " + streamName);
      return;
    }

    try {
      QueryHandle handle = exploreTableManager.enableStream(streamId, streamConfig);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (UnsupportedTypeException e) {
      LOG.error("Exception while generating create statement for stream {}", streamName, e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable t) {
      LOG.error("Got exception enabling explore on stream {}.", streamId, t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @POST
  @Path("streams/{stream}/disable")
  public void disableStream(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId, @PathParam("stream") String streamName) {
    Id.Stream streamId = Id.Stream.from(namespaceId, streamName);

    try {
      // throws io exception if there is no stream
      streamAdmin.getConfig(streamId);
    } catch (IOException e) {
      LOG.debug("Could not find stream {} to disable explore on.", streamName, e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Could not find stream " + streamName);
      return;
    }

    try {
      QueryHandle handle = exploreTableManager.disableStream(streamId);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable t) {
      LOG.error("Got exception disabling exploration for stream {}", streamId, t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/enable")
  public void enableDataset(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId, @PathParam("dataset") String datasetName) {
    Id.DatasetInstance datasetID = Id.DatasetInstance.from(namespaceId, datasetName);
    DatasetSpecification datasetSpec;
    try {
      datasetSpec = datasetFramework.getDatasetSpec(datasetID);
      if (datasetSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetID);
        return;
      }
    } catch (DatasetManagementException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error getting spec for dataset " + datasetID);
      return;
    }

    try {
      QueryHandle handle = exploreTableManager.enableDataset(datasetID, datasetSpec);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (ExploreException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error enabling explore on dataset " + datasetID);
    } catch (SQLException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "SQL exception while trying to enable explore on dataset " + datasetID);
    } catch (UnsupportedTypeException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Schema for dataset " + datasetID + " is not supported for exploration");
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/disable")
  public void disableDataset(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId, @PathParam("dataset") String datasetName) {

    LOG.debug("Disabling explore for dataset instance {}", datasetName);
    Id.DatasetInstance datasetID = Id.DatasetInstance.from(namespaceId, datasetName);
    DatasetSpecification spec;
    try {
      spec = datasetFramework.getDatasetSpec(datasetID);
      if (spec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetID);
        return;
      }
    } catch (DatasetManagementException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error getting spec for dataset " + datasetID);
      return;
    }

    try {
      QueryHandle handle = exploreTableManager.disableDataset(datasetID, spec);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception while trying to disable explore on dataset {}", datasetID, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("datasets/{dataset}/partitions")
  public void addPartition(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId, @PathParam("dataset") String datasetName) {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, datasetName);
    Dataset dataset;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      dataset = datasetInstantiator.getDataset(datasetInstanceId);

      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetInstanceId);
        return;
      }
    } catch (IOException e) {
      LOG.error("Exception instantiating dataset {}.", datasetInstanceId, e);
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

      QueryHandle handle = exploreTableManager.addPartition(datasetInstanceId, partitionKey, fsPath);
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
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("dataset") String datasetName) {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, datasetName);
    Dataset dataset;
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {

      dataset = datasetInstantiator.getDataset(datasetInstanceId);
      if (dataset == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + datasetInstanceId);
        return;
      }
    } catch (IOException e) {
      LOG.error("Exception instantiating dataset {}.", datasetInstanceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
        "Exception instantiating dataset " + datasetInstanceId);
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

      QueryHandle handle = exploreTableManager.dropPartition(datasetInstanceId, partitionKey);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }
}
