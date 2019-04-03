/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;

import java.util.Map;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * An app using dataset from another namespace
 */
public class CrossNsDatasetAccessApp extends AbstractApplication {

  public static final String APP_NAME = "WriterApp";
  public static final String SERVICE_NAME = "CrossNsService";
  public static final String OUTPUT_DATASET_NS = "output.dataset.ns";
  public static final String OUTPUT_DATASET_NAME = "output.dataset.name";

  @Override
  public void configure() {
    setName(APP_NAME);
    addService(SERVICE_NAME, new WriteHandler());
  }

  /**
   * A handler that gets deployed in one NS and write to a dataset in another NS.
   */
  public static final class WriteHandler extends AbstractHttpServiceHandler {

    @PUT
    @Path("/write/{data}")
    public void write(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("data") String data) {
      Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
      KeyValueTable table = getContext().getDataset(runtimeArgs.get(OUTPUT_DATASET_NS),
                                                    runtimeArgs.get(OUTPUT_DATASET_NAME));
      if (data.length() > 0) {
        table.write(data, data);
      }

      responder.sendStatus(200);
    }
  }
}
