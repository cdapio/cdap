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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A service endpoint to write to the {@link FakeDataset}.
 */
public class DatasetWriterService extends AbstractHttpServiceHandler {

  public static final String NAME = DatasetWriterService.class.getSimpleName();

  @UseDataSet(FakeApp.DS_NAME)
  private FakeDataset fakeDataset;

  @Path("/write")
  @POST
  public void write(HttpServiceRequest request, HttpServiceResponder responder) {
    Map<String, String> req = new Gson().fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                                                  new TypeToken<Map<String, String>>() { }.getType());
    req.forEach((k, v) -> fakeDataset.put(Bytes.toBytes(k), Bytes.toBytes(v)));
    responder.sendStatus(200);
  }
}
