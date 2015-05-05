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
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;

/**
 * An abstract class that provides common functionality for namespaced and non-namespaced ExploreMetadata handlers.
 */
public class AbstractExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExploreMetadataHttpHandler.class);
  private static final Gson GSON = new Gson();

  protected void handleResponseEndpointExecution(HttpRequest request, HttpResponder responder,
                                                 final EndpointCoreExecution<QueryHandle> execution) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        QueryHandle handle = execution.execute(request, responder);
        JsonObject json = new JsonObject();
        json.addProperty("handle", handle.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return null;
      }
    });
  }

  protected void genericEndpointExecution(HttpRequest request, HttpResponder responder,
                                          EndpointCoreExecution<Void> execution) {
    try {
      execution.execute(request, responder);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected <T> T decodeArguments(HttpRequest request, Class<T> argsType, T defaultValue) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return defaultValue;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      T args = GSON.fromJson(reader, argsType);
      return (args == null) ? defaultValue : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  /**
   * Represents the core execution of an endpoint.
   */
  protected static interface EndpointCoreExecution<T> {
    T execute(HttpRequest request, HttpResponder responder)
      throws IllegalArgumentException, SQLException, ExploreException, IOException;
  }

}
