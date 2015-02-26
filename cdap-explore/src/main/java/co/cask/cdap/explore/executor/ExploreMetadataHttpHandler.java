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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements explore metadata APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/data/explore")
public class ExploreMetadataHttpHandler extends AbstractExploreMetadataHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespacedExploreMetadataHttpHandler.class);

  private final ExploreService exploreService;

  @Inject
  public ExploreMetadataHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @POST
  @Path("jdbc/catalogs")
  public void getJDBCCatalogs(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get catalogs query.");
        return exploreService.getCatalogs();
      }
    });
  }

  @GET
  @Path("jdbc/info/{type}")
  public void getJDBCInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get info for {}", type);
        MetaDataInfo.InfoType infoType = MetaDataInfo.InfoType.fromString(type);
        MetaDataInfo metadataInfo = exploreService.getInfo(infoType);
        responder.sendJson(HttpResponseStatus.OK, metadataInfo);
        return null;
      }
    });
  }

  @POST
  @Path("jdbc/tableTypes")
  public void getJDBCTableTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get table types query.");
        return exploreService.getTableTypes();
      }
    });
  }

  @POST
  @Path("jdbc/types")
  public void getJDBCTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get type info query.");
        return exploreService.getTypeInfo();
      }
    });
  }

  // The following 2 endpoints are only for internal use and will be undocumented.
  // They are called by UnderlyingSystemNamespaceAdmin to create/destroy a database in Hive when a namespace in
  // CDAP is created/destroyed.
  // TODO: Consider addings ACLs to these operations.

  @PUT
  @Path("namespaces/{namespace-id}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        return exploreService.createNamespace(Id.Namespace.from(namespaceId));
      }
    });
  }

  @DELETE
  @Path("namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        return exploreService.deleteNamespace(Id.Namespace.from(namespaceId));
      }
    });
  }
}
