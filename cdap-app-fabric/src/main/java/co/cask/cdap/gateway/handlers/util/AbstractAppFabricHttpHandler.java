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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.proto.Instances;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract Class that contains commonly used methods for parsing Http Requests.
 */
public abstract class AbstractAppFabricHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppFabricHttpHandler.class);

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  public AbstractAppFabricHttpHandler(Authenticator authenticator) {
    super(authenticator);
  }

  protected int getInstances(HttpRequest request) throws IOException, NumberFormatException {
    return parseBody(request, Instances.class).getInstances();
  }

  @Nullable
  protected <T> T parseBody(HttpRequest request, Class<T> type) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      return GSON.fromJson(reader, type);
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse body on {} as {}", request.getUri(), type, e);
      throw e;
    } finally {
      reader.close();
    }
  }

  protected Map<String, String> decodeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, Constants.STRING_MAP_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }
}
