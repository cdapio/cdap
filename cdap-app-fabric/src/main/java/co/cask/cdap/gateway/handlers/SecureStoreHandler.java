/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.security.SecureStoreCreateRequest;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Exposes {@link org.apache.twill.api.SecureStore} and
 * {@link co.cask.cdap.api.security.store.SecureStoreManager} operations over HTTPS
 */
@Path(Constants.Gateway.API_VERSION_3 + "/security/store/namespaces/{namespace-id}")
public class SecureStoreHandler extends AbstractAppFabricHttpHandler {

  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  public SecureStoreHandler(SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Path("/key")
  @PUT
  public void create(HttpRequest httpRequest, HttpResponder httpResponder) throws BadRequestException {
    // Check authentication
    SecureStoreCreateRequest secureStoreCreateRequest = parseBody(httpRequest, SecureStoreCreateRequest.class);

    if (secureStoreCreateRequest == null) {
      throw new BadRequestException("Unable to parse the request.");
    }

    String name = secureStoreCreateRequest.getName();
    String description = secureStoreCreateRequest.getDescription();
    byte[] data = secureStoreCreateRequest.getData();
    if (name == null || data == null) {
      throw new BadRequestException("Name or data empty.");
    }
    try {
      secureStoreManager.put(name, data, description, secureStoreCreateRequest.getProperties());
    } catch (IOException e) {
      httpResponder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to store the item in the store.");
      return;
    }
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/keys/{key-name}")
  @DELETE
  public void delete(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("key-name") String name) {
    try {
      secureStoreManager.delete(name);
    } catch (IOException e) {
      httpResponder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to delete the key.");
      return;
    }
    httpResponder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/keys/{key-name}")
  @GET
  public void get(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("key-name") String name) {
    SecureStoreData secureStoreData;
    try {
      secureStoreData = secureStore.get(name);
    } catch (IOException e) {
      httpResponder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to get the key.");
      return;
    }
    httpResponder.sendJson(HttpResponseStatus.OK, secureStoreData.get());
  }

  @Path("/keys")
  @GET
  public void list(HttpRequest httpRequest, HttpResponder httpResponder) {
    try {
      httpResponder.sendJson(HttpResponseStatus.OK, secureStore.list());
    } catch (IOException e) {
      httpResponder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Unable to list the keys in the store.");
    }
  }
}
