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

package io.cdap.cdap.test;

import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.KeyValueTableDefinition;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.service.BasicService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application with a custom module, used for tests.
 */
public class AppUsingCustomModule extends AbstractApplication {
  @Override
  public void configure() {
    createDataset("myTable", "myKeyValueTable", DatasetProperties.EMPTY);
    addService(new BasicService("MyService", new TableHandler()));
  }

  /**
   * Dataset module which
   */
  public static class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
      KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("myKeyValueTable", tableDefinition);
      registry.add(keyValueTable);
    }
  }

  /**
   * HttpHandler to interact with "myTable".
   */
  public static class TableHandler extends AbstractHttpServiceHandler {

    @UseDataSet("myTable")
    private KeyValueTable table;

    @PUT
    @Path("{key}")
    public void set(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key) throws Exception {
      String value = Bytes.toString(request.getContent());
      table.write(key, value);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    }

    @GET
    @Path("{key}")
    public void get(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key) throws Exception {
      String value = Bytes.toString(table.read(key));
      responder.sendJson(value);
    }
  }
}
