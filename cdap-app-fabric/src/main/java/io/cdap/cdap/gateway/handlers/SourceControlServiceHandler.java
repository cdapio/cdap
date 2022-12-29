package io.cdap.cdap.gateway.handlers;


import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing source control operations.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/source-control")
public class SourceControlServiceHandler extends AbstractAppFabricHttpHandler {
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  SourceControlServiceHandler(NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @POST
  @Path("/initialise")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public BodyConsumer initialiseSourceControl(HttpRequest request, HttpResponder responder,
                                              @PathParam("namespace-id") final String namespaceId)
    throws BadRequestException, NamespaceNotFoundException, AccessException {
    NamespaceId ns = validateNamespace(namespaceId);
    responder.sendString(HttpResponseStatus.CREATED, "You have reached the initialise API!");
    return null
  }


  private NamespaceId validateNamespace(@Nullable String namespace)
    throws BadRequestException, NamespaceNotFoundException, AccessException {

    if (namespace == null) {
      throw new BadRequestException("Path parameter namespace-id cannot be empty");
    }

    NamespaceId namespaceId;
    try {
      namespaceId = new NamespaceId(namespace);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid namespace '%s'", namespace), e);
    }

    try {
      if (!namespaceId.equals(NamespaceId.SYSTEM)) {
        namespaceQueryAdmin.get(namespaceId);
      }
    } catch (NamespaceNotFoundException | AccessException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP calls to interact with namespaces.
      // In AppFabricServer, NamespaceAdmin is bound to DefaultNamespaceAdmin, which interacts directly with the MDS.
      // Hence, this exception will never be thrown
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }
}
