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

package co.cask.cdap.explore.executor;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * An abstract class that provides common functionality for namespaced and non-namespaced ExploreMetadata handlers.
 */
public class AbstractExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExploreMetadataHttpHandler.class);
  private static final Gson GSON = new Gson();

  protected <R extends HttpRequest> void handleEndpointExecution(R request, HttpResponder responder,
                                                                 final EndpointCoreExecution<R, QueryHandle> execution)
    throws ExploreException, IOException {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<R, Void>() {
      @Override
      public Void execute(R request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        QueryHandle handle = execution.execute(request, responder);
        JsonObject json = new JsonObject();
        json.addProperty("handle", handle.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json.toString());
        return null;
      }
    });
  }

  protected <R extends HttpRequest> void genericEndpointExecution(R request, HttpResponder responder,
                                                                  EndpointCoreExecution<R, Void> execution)
    throws ExploreException, IOException {
    try {
      execution.execute(request, responder);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    }
  }

  protected <T> T decodeArguments(FullHttpRequest request, Class<T> argsType, T defaultValue) throws IOException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      return defaultValue;
    }
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8)) {
      T args = GSON.fromJson(reader, argsType);
      return (args == null) ? defaultValue : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    }
  }

  /**
   * Represents the core execution of an endpoint.
   *
   * @param <R> type of the {@link HttpRequest} object
   * @param <T> type of result object from the {@link #execute(HttpRequest, HttpResponder)} method
   */
  protected interface EndpointCoreExecution<R extends HttpRequest, T> {
    T execute(R request, HttpResponder responder)
      throws IllegalArgumentException, SQLException, ExploreException, IOException;
  }

}
