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

package co.cask.cdap.service;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closeables;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A app for testing file upload through service to partitioned file set.
 */
public class FileUploadApp extends AbstractApplication {

  public static final String PFS_NAME = "files";
  public static final String KV_TABLE_NAME = "tracking";
  public static final String SERVICE_NAME = "pfs";

  @Override
  public void configure() {
    // A PFS for storing uploaded file
    createDataset(PFS_NAME, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
                  .setPartitioning(Partitioning.builder().addLongField("time").build())
                  .setInputFormat(TextInputFormat.class)
                  .build()
    );
    // A KV table for tracking chunks sizes
    createDataset(KV_TABLE_NAME, KeyValueTable.class);
    addService(SERVICE_NAME, new FileHandler());
  }

  public static final class FileHandler extends AbstractHttpServiceHandler {

    /**
     * Accepts file upload through the usage of {@link HttpContentConsumer}. It will store the file
     * under the given partition. It also verifies the upload content MD5.
     */
    @POST
    @Path("/upload/{dataset}/{partition}")
    public HttpContentConsumer upload(HttpServiceRequest request,
                                      HttpServiceResponder responder,
                                      @PathParam("dataset") String dataset,
                                      @PathParam("partition") long partition) throws Exception {
      final String md5 = request.getHeader("Content-MD5");
      if (md5 == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Missing header \"Content-MD5\"");
        return null;
      }

      // Construct the partition and the partition location
      PartitionKey partitionKey = PartitionKey.builder().addLongField("time", partition).build();
      PartitionedFileSet pfs = getContext().getDataset(dataset);
      final PartitionOutput partitionOutput = pfs.getPartitionOutput(partitionKey);
      final Location partitionDir = partitionOutput.getLocation();
      if (!partitionDir.mkdirs()) {
        responder.sendError(HttpURLConnection.HTTP_CONFLICT,
                            String.format("Partition for key '%s' already exists for dataset '%s'",
                                          partitionKey, dataset));
        return null;
      }

      final MessageDigest messageDigest = MessageDigest.getInstance("MD5");

      final Location location = partitionDir.append("upload-" + System.currentTimeMillis());
      final WritableByteChannel channel = Channels.newChannel(location.getOutputStream());

      // Handle upload content. The onReceived method is non-transactional.
      return new HttpContentConsumer() {

        @Override
        public void onReceived(final ByteBuffer chunk, Transactional transactional) throws Exception {
          transactional.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              KeyValueTable trackingTable = context.getDataset(KV_TABLE_NAME);
              trackingTable.increment(Bytes.toBytes(chunk.remaining()), 1L);
            }
          });
          chunk.mark();
          messageDigest.update(chunk);
          chunk.reset();
          channel.write(chunk);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          channel.close();
          String uploadedMd5 = BaseEncoding.base64().encode(messageDigest.digest());
          if (!md5.equals(uploadedMd5)) {
            throw new IllegalArgumentException("MD5 not match. Expected '" + md5 + "', received '" + uploadedMd5 + "'");
          }
          partitionOutput.addPartition();
          responder.sendStatus(HttpURLConnection.HTTP_OK);
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          try {
            Closeables.closeQuietly(channel);
            partitionDir.delete(true);
          } catch (IOException e) {
            // Nothing much can be done.
            throw Throwables.propagate(e);
          } finally {
            if (Throwables.getRootCause(failureCause) instanceof IllegalArgumentException) {
              responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
            } else {
              responder.sendStatus(HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
          }
        }
      };
    }
  }
}
