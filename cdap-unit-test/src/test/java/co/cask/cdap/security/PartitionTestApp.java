/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App to test partition creation/deletion from programs
 */
public class PartitionTestApp extends AbstractApplication {
  public static final String PFS_NAME = "pfs";
  public static final String PFS_SERVICE_NAME = "PartitionService";

  @Override
  public void configure() {
    addService(new PartitionService());
    // Create a partitioned file set, configure it to work with MapReduce and with Explore
    createDataset("pfs", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addStringField("partition").addIntField("sub-partition").build())
      // Properties for file set
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ",")
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("csv")
      .setExploreSchema("f1 STRING, f2 INT")
      .setDescription("App for testing authorization in partitioned filesets.")
      .build());
  }

  public static class PartitionService extends AbstractService {

    @Override
    protected void configure() {
      addHandler(new PartitionHandler());
    }

    public static class PartitionHandler extends AbstractHttpServiceHandler {
      @SuppressWarnings("unused")
      @UseDataSet("pfs")
      private PartitionedFileSet pfs;

      @POST
      @Path("partitions/{partition}/subpartitions/{sub-partition}")
      public void create(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("partition") String partition, @PathParam("sub-partition") int subPartition) {
        PartitionKey key = PartitionKey.builder()
          .addStringField("partition", partition)
          .addIntField("sub-partition", subPartition)
          .build();

        if (pfs.getPartition(key) != null) {
          responder.sendString(409, "Partition exists.", Charsets.UTF_8);
          return;
        }

        final PartitionOutput output = pfs.getPartitionOutput(key);
        try {
          final Location partitionDir = output.getLocation();
          if (!partitionDir.mkdirs()) {
            responder.sendString(409, "Partition exists.", Charsets.UTF_8);
            return;
          }

          final Location location = partitionDir.append("file");
          byte[] content = Bytes.toBytes(request.getContent());
          if (content == null) {
            responder.sendString(400, "No content", Charsets.UTF_8);
            return;
          }
          Files.write(Paths.get(location.toURI()), content);
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to write path '%s'. Reason: '%s'",
                                                 output.getRelativePath(), e.getMessage()));
          return;
        }

        output.addPartition();
        responder.sendString(200, "Successfully added partition", Charsets.UTF_8);
      }

      @GET
      @Path("partitions/{partition}/subpartitions/{sub-partition}")
      public void read(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("partition") String partition, @PathParam("sub-partition") int subPartition) {
        PartitionDetail partitionDetail = pfs.getPartition(PartitionKey.builder()
                                                             .addStringField("partition", partition)
                                                             .addIntField("sub-partition", subPartition)
                                                             .build());
        if (partitionDetail == null) {
          responder.sendString(404, "Partition not found.", Charsets.UTF_8);
          return;
        }

        try {
          responder.send(200, partitionDetail.getLocation().append("file"), "text/plain");
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to read path '%s'", partitionDetail.getRelativePath()));
        }
      }

      @DELETE
      @Path("partitions/{partition}/subpartitions/{sub-partition}")
      public void drop(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("partition") String partition, @PathParam("sub-partition") int subPartition) {

        PartitionKey key = PartitionKey.builder()
          .addStringField("partition", partition)
          .addIntField("sub-partition", subPartition)
          .build();

        if (pfs.getPartition(key) == null) {
          responder.sendString(404, "Partition not found.", Charsets.UTF_8);
          return;
        }

        pfs.dropPartition(key);

        responder.sendString(200, "Successfully dropped partition", Charsets.UTF_8);
      }
    }
  }
}
