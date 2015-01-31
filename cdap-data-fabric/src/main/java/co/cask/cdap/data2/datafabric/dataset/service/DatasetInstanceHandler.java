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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.HandlerException;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
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
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
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
@Path(Constants.Gateway.API_VERSION_2)
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(DatasetSpecification.class, new DatasetSpecificationAdapter())
    .create();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final ExploreFacade exploreFacade;

  private final CConfiguration conf;

  @Inject
  public DatasetInstanceHandler(DatasetTypeManager implManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, ExploreFacade exploreFacade,
                                CConfiguration conf) {
    this.opExecutorClient = opExecutorClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
    this.exploreFacade = exploreFacade;
    this.conf = conf;
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, final HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, instanceManager.getAll());
  }

  @GET
  @Path("/data/datasets/{name}")
  public void getInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("name") String name) {
    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      DatasetMeta info = new DatasetMeta(spec, implManager.getTypeInfo(spec.getType()), null);
      responder.sendJson(HttpResponseStatus.OK, info, DatasetMeta.class, GSON);
    }
  }

  /**
   * Creates a new Dataset instance.
   */
  @PUT
  @Path("/data/datasets/{name}")
  public void create(HttpRequest request, final HttpResponder responder,
                  @PathParam("name") String name) {
    DatasetInstanceConfiguration creationProperties = getInstanceConfiguration(request);

    LOG.info("Creating dataset {}, type name: {}, typeAndProps: {}",
             name, creationProperties.getTypeName(), creationProperties.getProperties());

    DatasetSpecification existing = instanceManager.get(name);
    if (existing != null) {
      String message = String.format("Cannot create dataset %s: instance with same name already exists %s",
                                     name, existing);
      LOG.info(message);
      responder.sendError(HttpResponseStatus.CONFLICT, message);
      return;
    }

    // Disable explore if the table already existed
    if (existing != null) {
      disableExplore(name);
    }
    
    if (!createDatasetInstance(creationProperties, name, responder, "create")) {
      return;
    }
    
    enableExplore(name, creationProperties);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Updates an existing Dataset specification properties  {@link DatasetInstanceConfiguration}
   * is constructed based on request and the Dataset instance is updated.
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  public void update(HttpRequest request, final HttpResponder responder,
                     @PathParam("name") String name) {
    DatasetInstanceConfiguration creationProperties = getInstanceConfiguration(request);

    LOG.info("Update dataset {}, type name: {}, typeAndProps: {}",
             name, creationProperties.getTypeName(), creationProperties.getProperties());
    DatasetSpecification existing = instanceManager.get(name);

    if (existing == null) {
      // update is true , but dataset instance does not exist, return 404.
      responder.sendError(HttpResponseStatus.NOT_FOUND,
                          String.format("Dataset Instance %s does not exist to update", name));
      return;
    }

    if (!existing.getType().equals(creationProperties.getTypeName())) {
      String  message = String.format("Cannot update dataset %s instance with a different type, existing type is %s",
                                      name, existing.getType());
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.CONFLICT, message);
      return;
    }

    disableExplore(name);
    
    if (!createDatasetInstance(creationProperties, name, responder, "update")) {
      return;
    }

    enableExplore(name, creationProperties);
    
    //caling admin upgrade, after updating specification
    executeAdmin(request, responder, name, "upgrade");
  }

  private DatasetInstanceConfiguration getInstanceConfiguration(HttpRequest request) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    DatasetInstanceConfiguration creationProperties = GSON.fromJson(reader, DatasetInstanceConfiguration.class);
    if (creationProperties.getProperties().containsKey(OrderedTable.PROPERTY_TTL)) {
      long ttl = TimeUnit.SECONDS.toMillis(Long.parseLong
        (creationProperties.getProperties().get(OrderedTable.PROPERTY_TTL)));
      creationProperties.getProperties().put(OrderedTable.PROPERTY_TTL, String.valueOf(ttl));
    }
    return  creationProperties;
  }

  private boolean createDatasetInstance(DatasetInstanceConfiguration creationProperties,
                                        String name, HttpResponder responder, String operation) {
    DatasetTypeMeta typeMeta = implManager.getTypeInfo(creationProperties.getTypeName());
    if (typeMeta == null) {
      String message = String.format("Cannot %s dataset %s: unknown type %s",
                                     operation, name, creationProperties.getTypeName());
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.NOT_FOUND, message);
      return false;
    }
    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec;
    try {
      spec = opExecutorClient.create(name, typeMeta,
                                     DatasetProperties.builder().addAll(creationProperties.getProperties()).build());
    } catch (Exception e) {
      String msg = String.format("Cannot %s dataset %s of type %s: executing create() failed, reason: %s",
                                 operation, name, creationProperties.getTypeName(), e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    instanceManager.add(spec);
    return true;
  }

  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, final HttpResponder responder,
                   @PathParam("name") String name) {
    LOG.info("Deleting dataset {}", name);

    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      if (!dropDataset(spec)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
    } catch (Exception e) {
      String msg = String.format("Cannot delete dataset %s: executing delete() failed, reason: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, final HttpResponder responder,
                           @PathParam("name") String instanceName,
                           @PathParam("method") String method) {

    try {
      Object result = null;
      String message = null;

      // NOTE: one cannot directly call create and drop, instead this should be called thru
      //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
      if (method.equals("exists")) {
        result = opExecutorClient.exists(instanceName);
      } else if (method.equals("truncate")) {
        opExecutorClient.truncate(instanceName);
      } else if (method.equals("upgrade")) {
        opExecutorClient.upgrade(instanceName);
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
  public void executeDataOp(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String instanceName,
                            @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Drops a dataset.
   * @param spec specification of dataset to be dropped.
   * @return true if dropped successfully, false if dataset is not found.
   * @throws Exception on error.
   */
  private boolean dropDataset(DatasetSpecification spec) throws Exception {
    String name = spec.getName();

    disableExplore(name);
    
    if (!instanceManager.delete(name)) {
      return false;
    }

    opExecutorClient.drop(spec, implManager.getTypeInfo(spec.getType()));
    return true;
  }
  
  private void disableExplore(String name) {
    // Disable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.disableExploreDataset(name);
    } catch (Exception e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  private void enableExplore(String name, DatasetInstanceConfiguration creationProperties) {
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.enableExploreDataset(name);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 name, creationProperties.getProperties(), e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }
  
  /**
   * Adapter for {@link co.cask.cdap.api.dataset.DatasetSpecification}
   */
  private static final class DatasetSpecificationAdapter implements JsonSerializer<DatasetSpecification> {

    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<SortedMap<String, String>>() { }.getType();
    private static final Maps.EntryTransformer<String, String, String> TRANSFORM_DATASET_PROPERTIES =
      new Maps.EntryTransformer<String, String, String>() {
        @Override
        public String transformEntry(String key, String value) {
          if (key.equals(OrderedTable.PROPERTY_TTL)) {
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
