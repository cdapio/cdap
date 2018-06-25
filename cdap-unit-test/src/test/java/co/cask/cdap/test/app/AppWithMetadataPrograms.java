/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App which has programs interacting with cdap metadata for tests
 */
public class AppWithMetadataPrograms extends AbstractApplication {
  public static final String APP_NAME = "AppWithMetadataPrograms";
  static final String METADATA_SERVICE_NAME = "MetadataService";
  static final String METADATA_SERVICE_DATASET = "MetadataServiceDataset";

  @Override
  public void configure() {
    setName(APP_NAME);
    addService(new BasicService(METADATA_SERVICE_NAME, new MetadataHandler()));
  }

  public static final class MetadataHandler extends AbstractHttpServiceHandler {
    @GET
    @Path("metadata/{dataset}")
    public void ping(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("dataset") String dataset) {
      Map<MetadataScope, Metadata> metadata =
        getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
      responder.sendJson(HttpResponseStatus.OK.code(), metadata);
    }

    @Override
    protected void configure() {
      createDataset(METADATA_SERVICE_DATASET, KeyValueTable.class);
    }
  }
}
