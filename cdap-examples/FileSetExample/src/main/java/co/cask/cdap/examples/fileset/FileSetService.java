/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
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
import javax.ws.rs.QueryParam;

/**
 * A Service to uploads files to, or downloads files from, the "lines" and "counts" file sets.
 */
public class FileSetService extends AbstractService {

  @Override
  protected void configure() {
    setName("FileSetService");
    setDescription("A Service to uploads files to, or downloads files from, the \"lines\" and \"counts\" file sets.");
    setInstances(1);
    addHandler(new FileSetHandler());
  }

  /**
   * A handler that allows reading and writing files.
   */
  public static class FileSetHandler extends AbstractHttpServiceHandler {

    @UseDataSet("lines")
    private FileSet lines;

    @UseDataSet("counts")
    private FileSet counts;

    @GET
    @Path("{fileSet}")
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("fileSet") String set, @QueryParam("path") String filePath) {

      FileSet fileSet;
      try {
        fileSet = getContext().getDataset(set);
      } catch (DatasetInstantiationException e) {
        responder.sendError(400, String.format("Invalid file set name '%s'", set));
        return;
      }

      Location location = fileSet.getLocation(filePath);
      ByteBuffer content;
      try {
        content = ByteBuffer.wrap(ByteStreams.toByteArray(location.getInputStream()));
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to read path '%s' in file set '%s'", filePath, set));
        return;
      }
      responder.send(200, content, "application/octet-stream", ImmutableMultimap.<String, String>of());
    }

    @PUT
    @Path("{fileSet}")
    public void write(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("fileSet") String set, @QueryParam("path") String filePath) {

      FileSet fileSet;
      try {
        fileSet = getContext().getDataset(set);
      } catch (DatasetInstantiationException e) {
        responder.sendError(400, String.format("Invalid file set name '%s'", set));
        return;
      }

      Location location = fileSet.getLocation(filePath);

      try {
        WritableByteChannel channel = Channels.newChannel(location.getOutputStream());
        try {
          channel.write(request.getContent());
        } finally {
          channel.close();
        }
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'", filePath, set));
        return;
      }
      responder.sendStatus(200);
    }
  }
}
