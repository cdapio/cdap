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

import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetInstanceHandler extends AbstractHttpHandler {

  private final DatasetInstanceService instanceService;

  @Inject
  public DatasetInstanceHandler(DatasetInstanceService instanceService) {
    this.instanceService = instanceService;
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, ConversionHelpers.spec2Summary(
      instanceService.list(ConversionHelpers.toNamespaceId(namespaceId))));
  }

  /**
   * Gets the {@link DatasetMeta} for a dataset instance.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @param owners a list of owners of the dataset instance, in the form @{code <type>::<id>}
   *               (e.g. "program::namespace:default/application:PurchaseHistory/program:flow:PurchaseFlow")
   * @throws Exception if the dataset instance was not found
   */
  @GET
  @Path("/data/datasets/{name}")
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace-id") String namespaceId,
                  @PathParam("name") String name,
                  @QueryParam("owner") List<String> owners) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       instanceService.get(ConversionHelpers.toDatasetInstanceId(namespaceId, name),
                                           ConversionHelpers.strings2ProgramIds(owners)),
                       DatasetMeta.class);
  }

  /**
   * Creates a new dataset instance.
   *
   * @param namespaceId namespace of the new dataset instance
   * @param name name of the new dataset instance
   */
  @PUT
  @Path("/data/datasets/{name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {

    DatasetInstanceConfiguration creationProperties = ConversionHelpers.getInstanceConfiguration(request);
    try {
      instanceService.create(namespaceId, name, creationProperties);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (DatasetAlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (DatasetTypeNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (HandlerException e) {
      responder.sendString(e.getFailureStatus(), e.getMessage());
    }
  }

  @GET
  @Path("/data/datasets/{name}/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String name) throws Exception {
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    responder.sendJson(HttpResponseStatus.OK,
                       instanceService.getOriginalProperties(instance),
                       new TypeToken<Map<String, String>>() { }.getType());
  }

  /**
   * Updates an existing dataset specification properties.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @throws Exception
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void update(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    Map<String, String> properties = ConversionHelpers.getProperties(request);
    instanceService.update(instance, properties);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Deletes a dataset instance, which also deletes the data owned by it.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @throws Exception
   */
  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("name") String name) throws Exception {
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    instanceService.drop(instance);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/data/datasets")
  public void dropAll(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId) throws Exception {
    instanceService.dropAll(ConversionHelpers.toNamespaceId(namespaceId));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @param method the admin operation to execute (e.g. "exists", "truncate", "upgrade")
   * @throws Exception
   */
  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                           @PathParam("name") String name,
                           @PathParam("method") String method) throws Exception {
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    try {
      DatasetAdminOpResponse response = instanceService.executeAdmin(instance, method);
      responder.sendJson(HttpResponseStatus.OK, response);
    } catch (HandlerException e) {
      responder.sendStatus(e.getFailureStatus());
    }
  }

  /**
   * Executes a data operation on a dataset instance. Not yet implemented.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @param method the data operation to execute
   */
  @POST
  @Path("/data/datasets/{name}/data/{method}")
  public void executeDataOp(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String name, @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }
}
