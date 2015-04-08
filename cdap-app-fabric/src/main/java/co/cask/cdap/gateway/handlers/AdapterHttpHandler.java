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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.CannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterAlreadyExistsException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.ApplicationTemplateInfo;
import co.cask.cdap.internal.app.runtime.adapter.InvalidAdapterOperationException;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for managing adapter lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AdapterHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterHttpHandler.class);

  private final AdapterService adapterService;
  private final NamespaceAdmin namespaceAdmin;
  @Inject
  public AdapterHttpHandler(Authenticator authenticator, AdapterService adapterService,
                            NamespaceAdmin namespaceAdmin) {
    super(authenticator);
    this.namespaceAdmin = namespaceAdmin;
    this.adapterService = adapterService;
  }

  /**
   * Deploy a template. Allows users to update a template, but leaves upgrade specifics to the user.
   * For example, if a program is renamed or new arguments are required, users must handle recreation of
   * adapters themselves.
   */
  @PUT
  @Path("/templates/{template-id}")
  public void deployTemplate(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("template-id") String templateId) {
    if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Namespace '%s' does not exist.", namespaceId));
      return;
    }
    try {
      adapterService.deployTemplate(Id.Namespace.from(namespaceId), templateId);
      responder.sendString(HttpResponseStatus.OK, "Deploy Complete");
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Template '%s' does not exist.", templateId));
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("Template '%s' is invalid: %s", templateId, e.getMessage()));
    } catch (Exception e) {
      LOG.error("Exception while trying to deploy template {} in namespace {}.", templateId, namespaceId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Internal error while trying to deploy template.");
    }
  }

  /**
   * Retrieves all adapters in a given namespace.
   */
  @GET
  @Path("/adapters")
  public void listAdapters(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) {
    if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Namespace '%s' does not exist.", namespaceId));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, adapterService.getAdapters(Id.Namespace.from(namespaceId)));
  }

  /**
   * Retrieves an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}")
  public void getAdapter(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("adapter-id") String adapterName) {
    try {
      AdapterSpecification adapterSpec = adapterService.getAdapter(Id.Namespace.from(namespaceId), adapterName);
      responder.sendJson(HttpResponseStatus.OK, adapterSpec);
    } catch (AdapterNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    }
  }

  /**
   * Starts/stops an adapter
   */
  @POST
  @Path("/adapters/{adapter-id}/{action}")
  public void startStopAdapter(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId,
                               @PathParam("action") String action) {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    try {
      if ("start".equals(action)) {
        adapterService.startAdapter(namespace, adapterId);
      } else if ("stop".equals(action)) {
        adapterService.stopAdapter(namespace, adapterId);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             String.format("Invalid adapter action: %s. Possible actions: ['start', 'stop'].", action));
        return;
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (InvalidAdapterOperationException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (SchedulerException e) {
      LOG.error("Scheduler error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterId, action, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } catch (Throwable t) {
      LOG.error("Error in namespace '{}' for adapter '{}' with action '{}'", namespaceId, adapterId, action, t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Retrieves the status of an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}/status")
  public void getAdapterStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId) {
    try {
      responder.sendString(HttpResponseStatus.OK,
                           adapterService.getAdapterStatus(Id.Namespace.from(namespaceId), adapterId).toString());
    } catch (AdapterNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    }
  }

  /**
   * Deletes an adapter
   */
  @DELETE
  @Path("/adapters/{adapter-id}")
  public void deleteAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) {
    try {
      adapterService.removeAdapter(Id.Namespace.from(namespaceId), adapterName);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (CannotBeDeletedException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (NotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (Throwable t) {
      LOG.error("Error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterName, "delete", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Create an adapter.
   */
  @PUT
  @Path("/adapters/{adapter-id}")
  public void createAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName) {

    AdapterConfig config;
    try {
      config = parseBody(request, AdapterConfig.class);
      if (config == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid adapter config");
        return;
      }
      if (config.getTemplate() == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "A template must be given in the adapter config");
        return;
      }
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid adapter config: " + e.getMessage());
      return;
    }
    Id.Namespace namespace = Id.Namespace.from(namespaceId);

    try {
      if (!namespaceAdmin.hasNamespace(namespace)) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             String.format("Create adapter failed - namespace '%s' does not exist.", namespaceId));
        return;
      }

      // Validate the adapter
      String templateName = config.getTemplate();
      ApplicationTemplateInfo applicationTemplateInfo = adapterService.getApplicationTemplateInfo(templateName);
      if (applicationTemplateInfo == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("App template %s not found", templateName));
        return;
      }

      adapterService.createAdapter(namespace, adapterName, config);
      responder.sendString(HttpResponseStatus.OK, String.format("Adapter: %s is created", adapterName));
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (AdapterAlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (Throwable th) {
      LOG.error("Failed to deploy adapter", th);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, th.getMessage());
    }
  }
}
