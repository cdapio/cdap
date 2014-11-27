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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.namespace.NamespaceMetadataStore;
import co.cask.common.http.HttpRequest;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link HttpHandler} for handling REST call to stream endpoints.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class NamespaceHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHttpHandler.class);

  // will need a handle for MDS here
  private NamespaceMetadataStore namespaceMetadataStore;

//  @Inject
//  public NamespaceHttpHandler(Authenticator authenticator) {
//    super(authenticator);
//  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    System.out.println("$$$$$$$$$$$$$$$ Getting all namespaces $$$$$$$$$$$$$$$$$$$$$$$");
    LOG.debug("Listing namespaces");
  }

  @PUT
  @Path("/namespaces/{namespace}")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    System.out.println("$$$$$$$$$$$$$$$$ Creating namespace " + namespace + " $$$$$$$$$$$$$$$$$$");
    LOG.debug("Creating namespace {}", namespace);
  }

  @DELETE
  @Path("/namespaces/{namespace}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace") String namespace) {
    System.out.println("$$$$$$$$$$$$$$$ Deleting namespace " + namespace + " $$$$$$$$$$$$$$$$$$$$");
    LOG.debug("Deleting namespace {}", namespace);
  }
}
