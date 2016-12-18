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

package co.cask.cdap.examples.sportresults;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A service for uploading sport results for a given league and season.
 */
public class UploadService extends AbstractService {

  @Override
  protected void configure() {
    setName("UploadService");
    setDescription("A service for uploading sport results for a given league and season.");
    setInstances(1);
    addHandler(new UploadHandler());
  }

  /**
   * A handler that allows reading and writing files.
   */
  public static class UploadHandler extends AbstractHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UploadHandler.class);

    @UseDataSet("results")
    private PartitionedFileSet results;

    @GET
    @Path("leagues/{league}/seasons/{season}")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("league") final String league,
                     @PathParam("season") final int season) throws TransactionFailureException {

      final PartitionKey key = PartitionKey.builder()
        .addStringField("league", league)
        .addIntField("season", season)
        .build();
      final AtomicReference<PartitionDetail> partitionDetail = new AtomicReference<>();

      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          partitionDetail.set(results.getPartition(key));
        }
      });
      if (partitionDetail.get() == null) {
        responder.sendString(404, "Partition not found.", Charsets.UTF_8);
        return;
      }

      try {
        responder.send(200, partitionDetail.get().getLocation().append("file"), "text/plain");
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to read path '%s'", partitionDetail.get().getRelativePath()));
      }
    }

    @PUT
    @Path("leagues/{league}/seasons/{season}")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public HttpContentConsumer write(HttpServiceRequest request, HttpServiceResponder responder,
                                     @PathParam("league") String league, @PathParam("season") int season)
      throws TransactionFailureException {

      final PartitionKey key = PartitionKey.builder()
        .addStringField("league", league)
        .addIntField("season", season)
        .build();
      final AtomicReference<PartitionDetail> partitionDetail = new AtomicReference<>();

      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          partitionDetail.set(results.getPartition(key));
        }
      });

      if (partitionDetail.get() != null) {
        responder.sendString(409, "Partition exists.", Charsets.UTF_8);
        return null;
      }

      final PartitionOutput output = results.getPartitionOutput(key);
      try {
        final Location partitionDir = output.getLocation();
        if (!partitionDir.mkdirs()) {
          responder.sendString(409, "Partition exists.", Charsets.UTF_8);
          return null;
        }

        final Location location = partitionDir.append("file");
        final WritableByteChannel channel = Channels.newChannel(location.getOutputStream());
        return new HttpContentConsumer() {
          @Override
          public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
            channel.write(chunk);
          }

          @Override
          public void onFinish(HttpServiceResponder responder) throws Exception {
            channel.close();
            output.addPartition();
            responder.sendStatus(200);
          }

          @Override
          public void onError(HttpServiceResponder responder, Throwable failureCause) {
            Closeables.closeQuietly(channel);
            try {
              partitionDir.delete(true);
            } catch (IOException e) {
              LOG.warn("Failed to delete partition directory '{}'", partitionDir, e);
            }
            LOG.debug("Unable to write path {}", location, failureCause);
            responder.sendError(400, String.format("Unable to write path '%s'. Reason: '%s'",
                                                   location, failureCause.getMessage()));
          }
        };
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s'. Reason: '%s'",
                                               output.getRelativePath(), e.getMessage()));
        return null;
      }
    }
  }
}
