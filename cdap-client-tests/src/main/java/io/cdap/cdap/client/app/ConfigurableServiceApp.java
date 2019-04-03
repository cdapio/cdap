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

package io.cdap.cdap.client.app;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class ConfigurableServiceApp extends AbstractApplication<ConfigurableServiceApp.ServiceConfig> {

  public static final String NAME = "TestServiceApp";
  public static final String SERVICE_NAME_BASE = "service";

  @Override
  public void configure() {
    setName(NAME);

    ServiceConfig config = getConfig();
    for (int i = 0; i < config.services; i++) {
      addService(SERVICE_NAME_BASE + i, new PingHandler());
    }
  }

  /**
   * Ping handler
   */
  public static final class PingHandler extends AbstractHttpServiceHandler {

    @Path("/ping")
    @GET
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }

  /**
   * App configuration to determine how many services to add
   */
  public static final class ServiceConfig extends Config {
    private int services;

    public ServiceConfig(int services) {
      this.services = services;
    }
  }
}
