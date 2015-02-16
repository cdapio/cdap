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
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.metrics.store.MetricStore;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Class for handling batch requests for metrics data.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/metrics")
public final class BatchMetricsHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BatchMetricsHandler.class);
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Gson GSON = new Gson();

  private final MetricStoreRequestExecutor requestExecutor;

  @Inject
  public BatchMetricsHandler(Authenticator authenticator, MetricStore metricStore) {
    super(authenticator);
    this.requestExecutor = new MetricStoreRequestExecutor(metricStore);
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    LOG.info("Starting BatchMetricsHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    LOG.info("Stopping BatchMetricsHandler");
  }

  @POST
  public void handleBatch(HttpRequest request, HttpResponder responder) throws IOException {
    if (!CONTENT_TYPE_JSON.equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      responder.sendString(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Only " + CONTENT_TYPE_JSON + " is supported.");
      return;
    }

    JsonArray output = new JsonArray();

    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
    String currPath = "";
    try {
      // decode requests
      List<URI> uris = GSON.fromJson(reader, new TypeToken<List<URI>>() { }.getType());
      LOG.trace("Requests: {}", uris);
      for (URI uri : uris) {
        currPath = uri.toString();
        CubeQuery query = MetricQueryParser.parse(uri);

        JsonObject json = new JsonObject();
        json.addProperty("path", uri.toString());
        json.add("result", requestExecutor.executeQuery(query));
        json.add("error", JsonNull.INSTANCE);

        output.add(json);
      }
      responder.sendJson(HttpResponseStatus.OK, output);
    } catch (MetricsPathException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid path '" + currPath + "': " + e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    } finally {
      reader.close();
    }
  }
}
