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
 */

package io.cdap.cdap.master.environment;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.AbstractService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.master.environment.plugin.ConstantCallable;

import java.util.Map;
import java.util.concurrent.Callable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * An application that uses a service that uses a plugin.
 */
public class ServiceWithPluginApp extends AbstractApplication<ServiceWithPluginApp.Conf> {
  public static final String NAME = ServiceWithPluginApp.class.getSimpleName();
  public static final String PLUGIN_TYPE = ConstantCallable.PLUGIN_TYPE;
  public static final String PLUGIN_ID = "id";

  @Override
  public void configure() {
    setName(NAME);
    addService(new ServiceWithPlugin());
    Conf conf = getConfig();
    usePlugin(PLUGIN_TYPE, conf.pluginName, PLUGIN_ID,
              PluginProperties.builder().addAll(conf.pluginProperties).build());
  }

  /**
   * Config for the app
   */
  public static class Conf extends Config {
    private final String pluginName;

    private final Map<String, String> pluginProperties;

    public Conf(String pluginName, Map<String, String> pluginProperties) {
      this.pluginName = pluginName;
      this.pluginProperties = pluginProperties;
    }
  }

  public static class ServiceWithPlugin extends AbstractService {
    public static final String NAME = ServiceWithPlugin.class.getSimpleName();

    @Override
    protected void configure() {
      setName(NAME);
      addHandler(new HandlerWithPlugin());
    }
  }

  /**
   * A handler that uses a plugin
   */
  public static class HandlerWithPlugin extends AbstractHttpServiceHandler {

    @GET
    @Path("call")
    public void call(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
      Callable<String> plugin = getContext().newPluginInstance(PLUGIN_ID);
      responder.sendString(plugin.call());
    }
  }
}
