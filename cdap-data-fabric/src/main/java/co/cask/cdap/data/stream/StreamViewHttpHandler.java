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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements view APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace}")
public class StreamViewHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamViewHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private final StreamAdmin admin;

  @Inject
  public StreamViewHttpHandler(StreamAdmin admin) {
    this.admin = admin;
  }

  @PUT
  @Path("/streams/{stream}/views/{view}")
  public void createOrUpdate(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace") String namespace,
                             @PathParam("stream") String stream,
                             @PathParam("view") String view) throws Exception {

    Id.Stream.View viewId;
    try {
      viewId = Id.Stream.View.from(namespace, stream, view);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }

    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()))) {
      ViewSpecification spec = GSON.fromJson(reader, ViewSpecification.class);
      if (spec == null) {
        throw new BadRequestException("Missing ViewSpecification in request body");
      }
      boolean created = admin.createOrUpdateView(viewId, spec);
      responder.sendStatus(created ? HttpResponseStatus.CREATED : HttpResponseStatus.OK);
    } catch (JsonSyntaxException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Couldn't decode body as view config JSON");
    } catch (IOException e) {
      LOG.warn("Error closing InputStreamReader", e);
    }
  }

  @DELETE
  @Path("/streams/{stream}/views/{view}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace") String namespace,
                     @PathParam("stream") String stream,
                     @PathParam("view") String view) throws Exception {

    Id.Stream.View viewId = Id.Stream.View.from(namespace, stream, view);
    admin.deleteView(viewId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/streams/{stream}/views")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace") String namespace,
                   @PathParam("stream") String stream) throws Exception {

    Id.Stream streamId = Id.Stream.from(namespace, stream);
    Collection<String> list = Collections2.transform(
      admin.listViews(streamId), new Function<Id.Stream.View, String>() {
        @Override
        public String apply(Id.Stream.View input) {
          return input.getId();
        }
      });
    responder.sendJson(HttpResponseStatus.OK, list);
  }

  @GET
  @Path("/streams/{stream}/views/{view}")
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace") String namespace,
                  @PathParam("stream") String stream,
                  @PathParam("view") String view) throws Exception {

    Id.Stream.View viewId = Id.Stream.View.from(namespace, stream, view);
    ViewDetail detail = new ViewDetail(viewId.getId(), admin.getView(viewId));
    responder.sendJson(HttpResponseStatus.OK, detail, ViewDetail.class, GSON);
  }
}
