/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.HandlerException;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(DatasetSpecification.class, new DatasetSpecificationAdapter())
    .create();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final ExploreFacade exploreFacade;
  private final boolean allowDatasetUncheckedUpgrade;

  @Inject
  public DatasetInstanceHandler(DatasetTypeManager implManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, ExploreFacade exploreFacade, CConfiguration conf) {
    this.opExecutorClient = opExecutorClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
    this.exploreFacade = exploreFacade;
    this.allowDatasetUncheckedUpgrade = conf.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  // the v2 version of the list API, which returns a collection of DatasetSpecification instead of
  // a collection of DatasetSpecificationSummary
  void v2list(HttpResponder responder, String namespaceId) {
    responder.sendJson(HttpResponseStatus.OK, instanceManager.getAll(Id.Namespace.from(namespaceId)));
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    List<DatasetSpecificationSummary> datasetSummaries = Lists.newArrayList();
    for (DatasetSpecification spec : instanceManager.getAll(Id.Namespace.from(namespaceId))) {
      datasetSummaries.add(new DatasetSpecificationSummary(spec.getName(), spec.getType(), spec.getProperties()));
    }
    responder.sendJson(HttpResponseStatus.OK, datasetSummaries);
  }

  @GET
  @Path("/data/datasets/{name}")
  public void getInfo(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                      @PathParam("name") String name) {
    DatasetSpecification spec = instanceManager.get(Id.DatasetInstance.from(namespaceId, name));
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      // try finding type info in the dataset's namespace first, then the system namespace
      DatasetTypeMeta typeMeta = getTypeInfo(Id.Namespace.from(namespaceId), spec.getType());
      if (typeMeta == null) {
        // Dataset type not found in the instance's namespace or the system namespace. Bail out.
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             String.format("Dataset type %s used by dataset %s not found", spec.getType(), name));
        return;
      }
      // typeMeta is guaranteed to be non-null now.
      DatasetMeta info = new DatasetMeta(spec, typeMeta, null);
      responder.sendJson(HttpResponseStatus.OK, info, DatasetMeta.class, GSON);
    }
  }

  /**
   * Creates a new Dataset instance.
   */
  @PUT
  @Path("/data/datasets/{name}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) {
    DatasetInstanceConfiguration creationProperties = getInstanceConfiguration(request);

    LOG.info("Creating dataset {}.{}, type name: {}, typeAndProps: {}",
             namespaceId, name, creationProperties.getTypeName(), creationProperties.getProperties());

    DatasetSpecification existing = instanceManager.get(Id.DatasetInstance.from(namespaceId, name));
    if (existing != null && !allowDatasetUncheckedUpgrade) {
      String message = String.format("Cannot create dataset %s.%s: instance with same name already exists %s",
                                     namespaceId, name, existing);
      LOG.info(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message);
      return;
    }

    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespaceId, name);
    // Disable explore if the table already existed
    if (existing != null) {
      disableExplore(datasetInstance);
    }

    if (!createDatasetInstance(creationProperties, namespaceId, name, responder, "create")) {
      return;
    }

    enableExplore(datasetInstance, creationProperties);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing Dataset specification properties  {@link DatasetInstanceConfiguration}
   * is constructed based on request and the Dataset instance is updated.
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  public void update(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) {
    DatasetInstanceConfiguration creationProperties = getInstanceConfiguration(request);

    LOG.info("Update dataset {}, type name: {}, typeAndProps: {}",
             name, creationProperties.getTypeName(), creationProperties.getProperties());
    DatasetSpecification existing = instanceManager.get(Id.DatasetInstance.from(namespaceId, name));

    if (existing == null) {
      // update is true , but dataset instance does not exist, return 404.
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Dataset Instance %s.%s does not exist to update", namespaceId, name));
      return;
    }

    if (!existing.getType().equals(creationProperties.getTypeName())) {
      String  message = String.format("Cannot update dataset %s.%s instance with a different type, existing type is %s",
                                      namespaceId, name, existing.getType());
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message);
      return;
    }

    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespaceId, name);
    disableExplore(datasetInstance);

    if (!createDatasetInstance(creationProperties, namespaceId, name, responder, "update")) {
      return;
    }

    enableExplore(datasetInstance, creationProperties);

    //caling admin upgrade, after updating specification
    executeAdmin(request, responder, namespaceId, name, "upgrade");
  }

  private DatasetInstanceConfiguration getInstanceConfiguration(HttpRequest request) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    DatasetInstanceConfiguration creationProperties = GSON.fromJson(reader, DatasetInstanceConfiguration.class);
    if (creationProperties.getProperties().containsKey(Table.PROPERTY_TTL)) {
      long ttl = TimeUnit.SECONDS.toMillis(Long.parseLong
        (creationProperties.getProperties().get(Table.PROPERTY_TTL)));
      creationProperties.getProperties().put(Table.PROPERTY_TTL, String.valueOf(ttl));
    }
    return  creationProperties;
  }

  private boolean createDatasetInstance(DatasetInstanceConfiguration creationProperties, String namespaceId,
                                        String name, HttpResponder responder, String operation) {
    String typeName = creationProperties.getTypeName();
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    DatasetTypeMeta typeMeta = getTypeInfo(namespace, typeName);
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      String message = String.format("Cannot %s dataset %s.%s: unknown type %s",
                                     operation, namespaceId, name, creationProperties.getTypeName());
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.NOT_FOUND, message);
      return false;
    }
    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec;
    try {
      spec = opExecutorClient.create(Id.DatasetInstance.from(namespaceId, name), typeMeta,
                                     DatasetProperties.builder().addAll(creationProperties.getProperties()).build());
    } catch (Exception e) {
      String msg = String.format("Cannot %s dataset %s.%s of type %s: executing create() failed, reason: %s",
                                 operation, namespaceId, name, creationProperties.getTypeName(), e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    instanceManager.add(namespace, spec);
    return true;
  }

  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("name") String name) {
    LOG.info("Deleting dataset {}.{}", namespaceId, name);
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, name);
    DatasetSpecification spec = instanceManager.get(Id.DatasetInstance.from(namespaceId, name));
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      if (!dropDataset(datasetInstanceId, spec)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (Exception e) {
      String msg = String.format("Cannot delete dataset %s.%s: executing delete() failed, reason: %s",
                                 namespaceId, name, e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                           @PathParam("name") String instanceName, @PathParam("method") String method) {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, instanceName);
    try {
      Object result = null;
      String message = null;

      // NOTE: one cannot directly call create and drop, instead this should be called thru
      //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
      if (method.equals("exists")) {
        result = opExecutorClient.exists(datasetInstanceId);
      } else if (method.equals("truncate")) {
        opExecutorClient.truncate(datasetInstanceId);
      } else if (method.equals("upgrade")) {
        opExecutorClient.upgrade(datasetInstanceId);
      } else {
        throw new HandlerException(HttpResponseStatus.NOT_FOUND, "Invalid admin operation: " + method);
      }

      DatasetAdminOpResponse response = new DatasetAdminOpResponse(result, message);
      responder.sendJson(HttpResponseStatus.OK, response);
    } catch (HandlerException e) {
      LOG.debug("Handler error", e);
      responder.sendStatus(e.getFailureStatus());
    } catch (Exception e) {
      LOG.error("Error executing admin operation {} for dataset instance {}", method, instanceName, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/datasets/{name}/data/{method}")
  public void executeDataOp(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String instanceName, @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Finds the {@link DatasetTypeMeta} for the specified dataset type name.
   * Search order - first in the specified namespace, then in the 'system' namespace from defaultModules
   *
   * @param namespaceId {@link Id.Namespace} for the specified namespace
   * @param typeName the name of the dataset type to search
   * @return {@link DatasetTypeMeta} for the type if found in either the specified namespace or in the system namespace,
   * null otherwise.
   * TODO: This may need to move to a util class eventually
   */
  @Nullable
  private DatasetTypeMeta getTypeInfo(Id.Namespace namespaceId, String typeName) {
    Id.DatasetType datasetTypeId = Id.DatasetType.from(namespaceId, typeName);
    DatasetTypeMeta typeMeta = implManager.getTypeInfo(datasetTypeId);
    if (typeMeta == null) {
      // Type not found in the instance's namespace. Now try finding it in the system namespace
      Id.DatasetType systemDatasetTypeId = Id.DatasetType.from(Constants.SYSTEM_NAMESPACE_ID, typeName);
      typeMeta = implManager.getTypeInfo(systemDatasetTypeId);
    }
    return typeMeta;
  }

  /**
   * Drops a dataset.
   * @param spec specification of dataset to be dropped.
   * @return true if dropped successfully, false if dataset is not found.
   * @throws Exception on error.
   */
  private boolean dropDataset(Id.DatasetInstance datasetInstanceId, DatasetSpecification spec) throws Exception {
    disableExplore(datasetInstanceId);

    if (!instanceManager.delete(datasetInstanceId)) {
      return false;
    }

    DatasetTypeMeta typeMeta = getTypeInfo(datasetInstanceId.getNamespace(), spec.getType());
    if (typeMeta == null) {
      return false;
    }
    opExecutorClient.drop(datasetInstanceId, typeMeta, spec);
    return true;
  }

  private void disableExplore(Id.DatasetInstance datasetInstance) {
    // Disable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.disableExploreDataset(datasetInstance);
    } catch (Exception e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 datasetInstance, e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  private void enableExplore(Id.DatasetInstance datasetInstance, DatasetInstanceConfiguration creationProperties) {
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.enableExploreDataset(datasetInstance);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 datasetInstance, creationProperties.getProperties(), e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  /**
   * Adapter for {@link DatasetSpecification}
   */
  private static final class DatasetSpecificationAdapter implements JsonSerializer<DatasetSpecification> {

    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<SortedMap<String, String>>() { }.getType();
    private static final Maps.EntryTransformer<String, String, String> TRANSFORM_DATASET_PROPERTIES =
      new Maps.EntryTransformer<String, String, String>() {
        @Override
        public String transformEntry(String key, String value) {
          if (key.equals(Table.PROPERTY_TTL)) {
            return String.valueOf(TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(value)));
          } else {
            return value;
          }
        }
      };

    @Override
    public JsonElement serialize(DatasetSpecification src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("name", src.getName());
      jsonObject.addProperty("type", src.getType());
      jsonObject.add("properties", context.serialize(Maps.transformEntries(src.getProperties(),
                                                     TRANSFORM_DATASET_PROPERTIES), MAP_STRING_STRING_TYPE));
      Type specsType = new TypeToken<SortedMap<String, DatasetSpecification>>() { }.getType();
      jsonObject.add("datasetSpecs", context.serialize(src.getSpecifications(), specsType));
      return jsonObject;
    }
  }
}
