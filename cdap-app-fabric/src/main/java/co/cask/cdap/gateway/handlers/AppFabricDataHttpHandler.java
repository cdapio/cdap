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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.store.NamespaceStore;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *  HttpHandler class for stream and dataset requests in app-fabric.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppFabricDataHttpHandler extends AbstractAppFabricHttpHandler {

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final NamespaceStore nsStore;

  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  AppFabricDataHttpHandler(Store store, NamespaceStore nsStore) {
    this.store = store;
    this.nsStore = nsStore;
  }

  /**
   * Returns a list of streams in a namespace.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespace) {

    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    Collection<StreamSpecification> specs = store.getAllStreams(namespaceId);
    List<StreamDetail> result = Lists.newArrayListWithExpectedSize(specs.size());
    for (StreamSpecification spec : specs) {
      result.add(new StreamDetail(spec.getName()));
    }
    if (result.isEmpty() && nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace '%s' not found.", namespace));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, result);
  }
}
