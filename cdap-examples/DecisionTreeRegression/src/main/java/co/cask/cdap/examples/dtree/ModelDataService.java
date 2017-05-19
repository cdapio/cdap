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

package co.cask.cdap.examples.dtree;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A Service that manages the training data for the model and fetches model metadata.
 */
public class ModelDataService extends AbstractService {
  public static final String SERVICE_NAME = "ModelDataService";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("Service to upload training data. Also retrieves information about the model.");
    addHandler(new ModelHandler());
  }

  /**
   * A handler that allows changing the training data and fetching model metadata.
   */
  public static class ModelHandler extends AbstractHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ModelHandler.class);

    private FileSet trainingData;
    private ObjectMappedTable<ModelMeta> modelMeta;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      trainingData = context.getDataset(DecisionTreeRegressionApp.TRAINING_DATASET);
      modelMeta = context.getDataset(DecisionTreeRegressionApp.MODEL_META);
    }

    @PUT
    @Path("labels")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public HttpContentConsumer addLabeledData(HttpServiceRequest request, HttpServiceResponder responder) {

      try {
        final Location location = trainingData.getBaseLocation().append(UUID.randomUUID().toString());
        final WritableByteChannel channel = Channels.newChannel(location.getOutputStream());
        return new HttpContentConsumer() {
          @Override
          public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
            channel.write(chunk);
          }

          @Override
          public void onFinish(HttpServiceResponder responder) throws Exception {
            // libsvm can't read multiple files, so always rename the location to 'labels'
            location.renameTo(trainingData.getBaseLocation().append("labels"));
            channel.close();
            responder.sendStatus(200);
          }

          @Override
          public void onError(HttpServiceResponder responder, Throwable failureCause) {
            Closeables.closeQuietly(channel);
            try {
              location.delete();
            } catch (IOException e) {
              LOG.warn("Failed to delete file '{}'", location, e);
            }
            LOG.debug("Unable to write file {}", location, failureCause);
            responder.sendError(400, String.format("Unable to write file '%s'. Reason: '%s'",
                                                   location, failureCause.getMessage()));
          }
        };
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write file. Reason: '%s'", e.getMessage()));
        return null;
      }
    }

    @GET
    @Path("models")
    public void getModels(HttpServiceRequest request, HttpServiceResponder responder) {
      List<String> models = new ArrayList<>();
      Scan scan = new Scan(null, null);
      try (CloseableIterator<KeyValue<byte[], ModelMeta>> iter = modelMeta.scan(scan)) {
        while (iter.hasNext()) {
          models.add(Bytes.toString(iter.next().getKey()));
        }
      }
      responder.sendJson(200, models);
    }

    @GET
    @Path("models/{model}")
    public void getModelMeta(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("model") final String model) {
      ModelMeta meta = modelMeta.read(model);
      if (meta == null) {
        responder.sendError(404, "Model " + model + " not found.");
      }
      responder.sendJson(200, meta);
    }
  }
}
