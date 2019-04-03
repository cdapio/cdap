/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.service;

import com.google.gson.Gson;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpContentConsumer;
import io.cdap.cdap.api.service.http.HttpContentProducer;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.internal.guava.reflect.TypeToken;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App for testing plugin instantiating at service runtime without registering any plugins at configure time.
 */
public class DynamicPluginServiceApp extends AbstractApplication {
  public static final String PLUGIN_TYPE = "supplier";
  public static final String SERVICE_NAME = "service";
  public static final String NAMESPACE_HEADER = "Namespace";

  @Override
  public void configure() {
    addService(new DynamicPluginService());
  }

  /**
   * Dynamic plugin service
   */
  public static class DynamicPluginService extends AbstractService {

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      addHandler(new DynamicPluginHandler());
    }
  }

  /**
   * Dynamic plugin handler
   */
  public static class DynamicPluginHandler extends AbstractHttpServiceHandler {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
    private boolean onFinishSuccessful = false;

    @POST
    @Path("plugins/{name}/apply")
    public void callPluginFunction(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("name") String name) {
      Map<String, String> properties = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                                     MAP_TYPE);
      PluginProperties pluginProperties = PluginProperties.builder()
        .addAll(properties)
        .build();

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(getNamespace(request));
      Function<PluginConfigurer, String> plugin =
        pluginConfigurer.usePlugin(PLUGIN_TYPE, name, UUID.randomUUID().toString(), pluginProperties);
      if (plugin == null) {
        responder.sendError(404, "Plugin " + name + " not found.");
        return;
      }

      responder.sendString(plugin.apply(pluginConfigurer));
    }

    // used to test that plugins can be used within a BodyProducer
    @POST
    @Path("producer")
    public void producePluginFunction(HttpServiceRequest request, HttpServiceResponder responder) {
      PluginRequest pluginRequest = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                                  PluginRequest.class);
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(getNamespace(request));

      HttpContentProducer producer = new HttpContentProducer() {
        private boolean done = false;

        @Override
        public ByteBuffer nextChunk(Transactional transactional) {
          if (done) {
            return ByteBuffer.allocate(0);
          }

          PluginProperties pluginProperties = PluginProperties.builder()
            .addAll(pluginRequest.goodProperties)
            .build();
          Function<PluginConfigurer, String> plugin = pluginConfigurer.usePlugin(PLUGIN_TYPE, pluginRequest.goodName,
                                                                                 UUID.randomUUID().toString(),
                                                                                 pluginProperties);
          if (plugin == null) {
            return ByteBuffer.allocate(0);
          }

          done = true;
          String result = plugin.apply(pluginConfigurer);
          return ByteBuffer.wrap(Bytes.toBytes(result));
        }

        @Override
        public void onFinish() {
          // check instantiating in onFinish()
          PluginProperties pluginProperties = PluginProperties.builder()
            .addAll(pluginRequest.goodProperties)
            .build();
          Function<PluginConfigurer, String> plugin = pluginConfigurer.usePlugin(PLUGIN_TYPE, pluginRequest.goodName,
                                                                                 UUID.randomUUID().toString(),
                                                                                 pluginProperties);
          onFinishSuccessful = plugin != null;
        }

        @Override
        public void onError(Throwable failureCause) {
          // no-op
        }

      };
      responder.send(200, producer, "text/plain");
    }

    // used to check if the plugin was instantiated successfully in the ContentProducer's onFinish method
    @GET
    @Path("onFinishSuccessful")
    public void onFinishSuccessful(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(Boolean.toString(onFinishSuccessful));
    }

    // used to test that plugins can be used within an HttpContentConsumer
    // the PluginRequest is read using the content consumer
    @POST
    @Path("consumer")
    public HttpContentConsumer callWithConsumer(HttpServiceRequest request, HttpServiceResponder responder) {
      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(getNamespace(request));
      return new HttpContentConsumer() {
        private byte[] body = new byte[0];
        private PluginRequest pluginRequest;

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) {
          body = Bytes.concat(body, Bytes.toBytes(chunk));
        }

        @Override
        public void onFinish(HttpServiceResponder responder) {
          pluginRequest = GSON.fromJson(Bytes.toString(body), PluginRequest.class);
          PluginProperties pluginProperties = PluginProperties.builder()
            .addAll(pluginRequest.goodProperties)
            .build();
          Function<PluginConfigurer, String> plugin = pluginConfigurer.usePlugin(PLUGIN_TYPE, pluginRequest.goodName,
                                                                                 UUID.randomUUID().toString(),
                                                                                 pluginProperties);
          if (plugin == null) {
            // throw an
            throw new IllegalArgumentException("Plugin does not exist");
          }
          responder.sendString(plugin.apply(pluginConfigurer));
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          PluginProperties pluginProperties = PluginProperties.builder()
            .addAll(pluginRequest.errorProperties)
            .build();
          Function<PluginConfigurer, String> plugin = pluginConfigurer.usePlugin(PLUGIN_TYPE, pluginRequest.errorName,
                                                                                 UUID.randomUUID().toString(),
                                                                                 pluginProperties);
          if (plugin == null) {
            responder.sendError(404, "error plugin not found");
            return;
          }
          responder.sendError(400, plugin.apply(pluginConfigurer));
        }
      };
    }

    private String getNamespace(HttpServiceRequest request) {
      String namespace = request.getHeader(NAMESPACE_HEADER);
      return namespace == null ? getContext().getNamespace() : namespace;
    }
  }

  /**
   * Request body of the 'contentconsumer' endpoint
   */
  public static class PluginRequest {
    private final String goodName;
    private final String errorName;
    private final Map<String, String> goodProperties;
    private final Map<String, String> errorProperties;

    public PluginRequest(String goodName, Map<String, String> goodProperties,
                         String errorName, Map<String, String> errorProperties) {
      this.goodName = goodName;
      this.errorName = errorName;
      this.goodProperties = goodProperties;
      this.errorProperties = errorProperties;
    }
  }
}
