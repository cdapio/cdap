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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class DummyApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("DummyApp");
    setDescription("DummyApp");
    addStream(new Stream("who"));
    createDataset("whom", KeyValueTable.class);
    addService(new Greeting());
  }


  /**
   * A {@link co.cask.cdap.api.service.Service} that creates a greeting using a user's name.
   */
  public static final class Greeting extends AbstractService {

    public static final String SERVICE_NAME = "Greeting";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("Service that creates a greeting using a user's name.");
      addHandler(new GreetingHandler());
    }
  }

  /**
   * Greeting Service handler.
   */
  public static final class GreetingHandler extends AbstractHttpServiceHandler {

    @UseDataSet("whom")
    private KeyValueTable whom;

    private Metrics metrics;

    @Path("greet")
    @GET
    public void greet(HttpServiceRequest request, HttpServiceResponder responder) {
      byte[] name = whom.read("");
      String toGreet = name != null ? new String(name, Charsets.UTF_8) : "World";
      if (toGreet.equals("Jane Doe")) {
        metrics.count("greetings.count.jane_doe", 1);
      }
      responder.sendString(String.format("Hello %s!", toGreet));
    }
  }
}
