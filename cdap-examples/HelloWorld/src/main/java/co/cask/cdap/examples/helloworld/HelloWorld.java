/*
 * Copyright Â© 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.helloworld;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class HelloWorld extends AbstractApplication {

  @Override
  public void configure() {
    setName("TestApp");
    setDescription("An application used for testing.");

    createDataset("myds", MyDataset.class, FileSetProperties.builder().setBasePath("tmp/myds").build());
    createDataset("files2", FileSet.class, FileSetProperties.builder().setBasePath("/temp/files2").build());

    addService(new FileService());
  }

  /**
   *
   */
  public static class FileService extends AbstractService {

    @Override
    protected void configure() {
      addHandler(new FileHandler());
    }

    /**
     *
     */
    public static class FileHandler extends AbstractHttpServiceHandler {

      @UseDataSet("myds")
      private MyDataset myds;

      @GET
      @Path("/seasons/{season}")
      public void read(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("season") long season) {
        Partition partitionDetail = myds.getTpfs().getPartitionByTime(season);
        if (partitionDetail == null) {
          responder.sendString(404, "Partition not found.", Charsets.UTF_8);
          return;
        }
        ByteBuffer content;
        try {
          Location location = partitionDetail.getLocation().append("file");
          content = ByteBuffer.wrap(ByteStreams.toByteArray(location.getInputStream()));
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to read path '%s'", partitionDetail.getRelativePath()));
          return;
        }
        responder.send(200, content, "text/plain", ImmutableMultimap.<String, String>of());
      }

      @PUT
      @Path("/seasons/{season}")
      public void write(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("season") long season) {
        if (myds.getTpfs().getPartitionByTime(season) != null) {
          responder.sendString(409, "Partition exists.", Charsets.UTF_8);
          return;
        }

        PartitionOutput output = myds.getTpfs().getPartitionOutput(season);
        try {
          Location location = output.getLocation().append("file");
          try (WritableByteChannel channel = Channels.newChannel(location.getOutputStream())) {
            channel.write(request.getContent());
          }
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to write path '%s'", output.getRelativePath()));
          return;
        }
        output.addPartition();
        responder.sendStatus(200);
      }

      @UseDataSet("files2")
      private FileSet files2;

      @GET
      @Path("/{ds}/{file}")
      public void getFile(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("ds") String datasetName, @PathParam("file") String fileName) {

        FileSet fileset = getFileSet(datasetName);
        Location location = fileset.getLocation(fileName);
        ByteBuffer content;
        try {
          content = ByteBuffer.wrap(ByteStreams.toByteArray(location.getInputStream()));
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to read path '%s' in dataset '%s'", fileName, datasetName));
          return;
        }
        responder.send(200, content, "application/octet-stream", ImmutableMultimap.<String, String>of());
      }

      @PUT
      @Path("/{ds}/{file}")
      public void putFile(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("ds") String datasetName, @PathParam("file") String fileName) {

        FileSet fileset = getFileSet(datasetName);
        Location location = fileset.getLocation(fileName);
        try {
          try (WritableByteChannel channel = Channels.newChannel(location.getOutputStream())) {
            channel.write(request.getContent());
          }
        } catch (IOException e) {
          responder.sendError(400, String.format("Unable to write path '%s' in dataset '%s'", fileName, datasetName));
          return;
        }
        responder.sendStatus(200);
      }

      private FileSet getFileSet(String datasetName) {
        if (datasetName.startsWith("files")) {
          return getContext().getDataset(datasetName);
        }
        if (datasetName.contains("pfs")) {
          PartitionedFileSet pfs = getContext().getDataset(datasetName);
          return pfs.getEmbeddedFileSet();
        }
        MyDataset myds = getContext().getDataset(datasetName);
        return myds.getTpfs().getEmbeddedFileSet();
      }
    }
  }

  /**
   *
   */
  public static class MyDataset extends AbstractDataset {

    private TimePartitionedFileSet tpfs;

    public MyDataset(DatasetSpecification spec,
                     @EmbeddedDataset("tpfs") TimePartitionedFileSet tpfs,
                     @EmbeddedDataset("table") Table table) {
      super(spec.getName(), tpfs, table);
      this.tpfs = tpfs;
    }

    public TimePartitionedFileSet getTpfs() {
      return tpfs;
    }
  }
}
