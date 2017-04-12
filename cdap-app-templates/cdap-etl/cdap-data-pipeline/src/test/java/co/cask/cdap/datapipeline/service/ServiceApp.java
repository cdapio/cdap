/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datapipeline.service;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Simple service app which exposes a service endpoint
 */
public class ServiceApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("ServiceApp");
    setDescription("A program which exposes a simple service");
    addStream(new Stream("who"));
    createDataset("whom", KeyValueTable.class, DatasetProperties.builder().setDescription("Store names").build());
    addService(new Name());
  }

  /**
   * A Service that checks if the name is stored in the kv dataset or not. If name does not exist it returns "null" or
   * returns same name
   */
  public static final class Name extends AbstractService {

    public static final String SERVICE_NAME = "Name";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("Service that checks if the name is stored in the kv dataset or not");
      addHandler(new NameHandler());
      addHandler(new NameSaveHandler());
    }
  }

  /**
   * Greeting Service handler.
   */
  public static final class NameSaveHandler extends AbstractHttpServiceHandler {

    @UseDataSet("whom")
    private KeyValueTable whom;

    @Path("name")
    @POST
    public void saveName(HttpServiceRequest request, HttpServiceResponder responder) {
      String body = Bytes.toString(request.getContent());
      whom.write(Bytes.toBytes(body), Bytes.toBytes(body));
      responder.sendStatus(200);
    }
  }

  /**
   * Greeting Service handler.
   */
  public static final class NameHandler extends AbstractHttpServiceHandler {

    @UseDataSet("whom")
    private KeyValueTable whom;

    @Path("name/{user}")
    @GET
    public void getName(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("user") String user) {
      byte[] name = whom.read(Bytes.toBytes(user));
      String toSend = name != null ? new String(name, Charsets.UTF_8) : "null";
      responder.sendString(String.format("%s", toSend));
    }
  }
}
