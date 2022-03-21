/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.explore.executor;

import com.google.common.base.Strings;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.explore.client.DisableExploreParameters;
import io.cdap.cdap.explore.client.EnableExploreParameters;
import io.cdap.cdap.explore.client.UpdateExploreParameters;
import io.cdap.cdap.explore.service.ExploreException;
import io.cdap.cdap.explore.service.ExploreTableManager;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.QueryHandle;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import javax.ws.rs.HeaderParam;
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
  private final SystemDatasetInstantiatorFactory datasetInstantiatorFactory;
  private final Impersonator impersonator;

  @Inject
  public ExploreExecutorHttpHandler(ExploreTableManager exploreTableManager,
                                    DatasetFramework datasetFramework,
                                    SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                                    Impersonator impersonator) {
    this.exploreTableManager = exploreTableManager;
    this.datasetFramework = datasetFramework;
    this.datasetInstantiatorFactory = datasetInstantiatorFactory;
    this.impersonator = impersonator;
  }

  @POST
  @Path("datasets/{dataset}/enable-internal")
  public void enableInternal(FullHttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace,
                             @PathParam("dataset") String datasetName)
    throws BadRequestException, IOException {

    EnableExploreParameters params = readEnableParameters(request);
    enableDataset(responder, new DatasetId(namespace, datasetName), params.getSpec(), params.isTruncating());
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/enable")
  public void enableDataset(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName) {
    DatasetId datasetId = new DatasetId(namespace, datasetName);
    DatasetSpecification datasetSpec = retrieveDatasetSpec(responder, datasetId);
    if (datasetSpec == null) {
      return; // this means the spec could not be retrieved and retrievedDatasetSpec() already responded
    }
    enableDataset(responder, datasetId, datasetSpec, false);
  }

  private DatasetSpecification retrieveDatasetSpec(HttpResponder responder, DatasetId datasetId) {
    DatasetSpecification datasetSpec;
    try {
      datasetSpec = datasetFramework.getDatasetSpec(datasetId);
      if (datasetSpec == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Dataset %s not found.", datasetId));
        return null;
      }
      return datasetSpec;
    } catch (DatasetManagementException e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error getting spec for dataset " + datasetId);
      return null;
    }
  }

  private void enableDataset(HttpResponder responder, final DatasetId datasetId,
                             final DatasetSpecification datasetSpec, final boolean truncating) {
    LOG.debug("Enabling explore for dataset instance {}", datasetId);
    try {
      QueryHandle handle = impersonator.doAs(datasetId, new Callable<QueryHandle>() {
        @Override
        public QueryHandle call() throws Exception {
          return exploreTableManager.enableDataset(datasetId, datasetSpec, truncating);
        }
      });
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json.toString());
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
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateDataset(FullHttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName)
    throws BadRequestException {

    final DatasetId datasetId = new DatasetId(namespace, datasetName);
    try {
      UpdateExploreParameters params = readUpdateParameters(request);
      final DatasetSpecification oldSpec = params.getOldSpec();
      final DatasetSpecification datasetSpec = params.getNewSpec();

      QueryHandle handle;
      if (oldSpec.equals(datasetSpec)) {
        handle = QueryHandle.NO_OP;
      } else {
        handle = impersonator.doAs(datasetId, new Callable<QueryHandle>() {
          @Override
          public QueryHandle call() throws Exception {
            return exploreTableManager.updateDataset(datasetId, datasetSpec, oldSpec);
          }
        });
      }
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json.toString());
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

  private static EnableExploreParameters readEnableParameters(FullHttpRequest request)
    throws BadRequestException, IOException {
    return doReadExploreParameters(request, EnableExploreParameters.class);
  }

  private static DisableExploreParameters readDisableParameters(FullHttpRequest request)
    throws BadRequestException, IOException {
    return doReadExploreParameters(request, DisableExploreParameters.class);
  }

  private static UpdateExploreParameters readUpdateParameters(FullHttpRequest request)
    throws BadRequestException, IOException {
    return doReadExploreParameters(request, UpdateExploreParameters.class);
  }

  private static <T> T doReadExploreParameters(FullHttpRequest request, Class<T> clz)
    throws BadRequestException, IOException {
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      return GSON.fromJson(reader, clz);
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

    final DatasetId datasetId = new DatasetId(namespace, datasetName);
    DatasetSpecification datasetSpec = retrieveDatasetSpec(responder, datasetId);
    if (datasetSpec == null) {
      return; // this means the spec could not be retrieved and retrievedDatasetSpec() already responded
    }
    disableDataset(responder, datasetId, datasetSpec);
  }

  /**
   * Disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("datasets/{dataset}/disable-internal")
  public void disableInternal(FullHttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace, @PathParam("dataset") String datasetName)
    throws BadRequestException, IOException {

    disableDataset(responder, new DatasetId(namespace, datasetName), readDisableParameters(request).getSpec());
  }

  private void disableDataset(HttpResponder responder, final DatasetId datasetId, final DatasetSpecification spec) {
    try {
      QueryHandle handle = impersonator.doAs(datasetId, new Callable<QueryHandle>() {
        @Override
        public QueryHandle call() throws Exception {
          return exploreTableManager.disableDataset(datasetId, spec);
        }
      });
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json.toString());
    } catch (Throwable e) {
      LOG.error("Got exception while trying to disable explore on dataset {}", datasetId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("datasets/{dataset}/partitions")
  public void addPartition(final FullHttpRequest request, final HttpResponder responder,
                           @PathParam("namespace-id") String namespace,
                           @PathParam("dataset") String datasetName,
                           @HeaderParam(Constants.Security.Headers.PROGRAM_ID) String programId) throws Exception {
    final DatasetId datasetId = new DatasetId(namespace, datasetName);
    propagateUserId(request);
    impersonator.doAs(getEntityToImpersonate(datasetId, programId), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        doPartitionOperation(request, responder, datasetId, new PartitionOperation() {
          @Override
          public QueryHandle submitOperation(PartitionKey partitionKey, Map<String, String> properties)
            throws ExploreException, SQLException {
            String fsPath = properties.get("path");
            if (fsPath == null) {
              responder.sendString(HttpResponseStatus.BAD_REQUEST, "path was not specified.");
              return null;
            }
            return exploreTableManager.addPartition(datasetId, properties, partitionKey, fsPath);
          }
        });
        return null;
      }
    });
  }

  abstract static class PartitionOperation {
    // returns null if no operation was submitted, such as if the properties are not sufficient
    @Nullable
    abstract QueryHandle submitOperation(PartitionKey partitionKey, Map<String, String> properties)
      throws ExploreException, SQLException;
  }

  private void doPartitionOperation(FullHttpRequest request, HttpResponder responder, DatasetId datasetId,
                                    PartitionOperation partitionOperation) {
    try (SystemDatasetInstantiator datasetInstantiator = datasetInstantiatorFactory.create()) {
      Dataset dataset;
      try {
        dataset = datasetInstantiator.getDataset(datasetId);
      } catch (Exception e) {
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

        Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()));
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
        QueryHandle handle = partitionOperation.submitOperation(partitionKey, properties);
        if (handle == null) {
          return;
        }
        JsonObject json = new JsonObject();
        json.addProperty("handle", handle.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json.toString());
      } finally {
        Closeables.closeQuietly(dataset);
      }
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }


  // this should really be a DELETE request. However, the partition key must be passed in the body
  // of the request, and that does not work with many HTTP clients, including Java's URLConnection.
  @POST
  @Path("datasets/{dataset}/deletePartition")
  public void dropPartition(final FullHttpRequest request, final HttpResponder responder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("dataset") String datasetName,
                            @HeaderParam(Constants.Security.Headers.PROGRAM_ID) String programId) throws Exception {
    final DatasetId datasetId = new DatasetId(namespace, datasetName);
    propagateUserId(request);
    impersonator.doAs(getEntityToImpersonate(datasetId, programId), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        doPartitionOperation(request, responder, datasetId, new PartitionOperation() {
          @Override
          public QueryHandle submitOperation(PartitionKey partitionKey, Map<String, String> properties)
            throws ExploreException, SQLException {
            return exploreTableManager.dropPartition(datasetId, properties, partitionKey);
          }
        });
        return null;
      }
    });
  }


  @POST
  @Path("datasets/{dataset}/concatenatePartition")
  public void concatenatePartition(final FullHttpRequest request, final HttpResponder responder,
                                   @PathParam("namespace-id") String namespace,
                                   @PathParam("dataset") String datasetName,
                                   @HeaderParam(Constants.Security.Headers.PROGRAM_ID) String programId)
    throws Exception {
    final DatasetId datasetId = new DatasetId(namespace, datasetName);
    propagateUserId(request);
    impersonator.doAs(getEntityToImpersonate(datasetId, programId), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        doPartitionOperation(request, responder, datasetId, new PartitionOperation() {
          @Override
          public QueryHandle submitOperation(PartitionKey partitionKey, Map<String, String> properties)
            throws ExploreException, SQLException {
            return exploreTableManager.concatenatePartition(datasetId, properties, partitionKey);
          }
        });
        return null;
      }
    });
  }

  private NamespacedEntityId getEntityToImpersonate(NamespacedEntityId entityId, String programId) {
    // if program id was passed then we impersonate the programId
    return Strings.isNullOrEmpty(programId) ? entityId : ProgramId.fromString(programId);
  }

  // propagate user id from the HTTP Request in the current thread
  private void propagateUserId(HttpRequest request) {
    String userId = request.headers().get(Constants.Security.Headers.USER_ID);
    if (userId != null) {
      LOG.debug("Propagating userId as {}", userId);
      SecurityRequestContext.setUserId(userId);
    }
  }
}
