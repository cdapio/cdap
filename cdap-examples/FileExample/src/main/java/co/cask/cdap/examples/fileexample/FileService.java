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

package co.cask.cdap.examples.fileexample;

import co.cask.cdap.api.annotation.UseDataSet;
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
 * A service that writes files to the "lines" file set and reads files from the "counts" file set.
 */
public class FileService extends AbstractService {

  @Override
  protected void configure() {
    setName("FileService");
    setDescription("A service to upload or download files in the \"lines\" and  \"counts\" file sets.");
    setInstances(1);
    addHandler(new FileHandler());
  }

  /**
   * A handler that allows reading and writing files.
   */
  public static class FileHandler extends AbstractHttpServiceHandler {

    @UseDataSet("lines")
    private FileSet lines;

    @UseDataSet("counts")
    private FileSet counts;

    @GET
    @Path("{fileSet}")
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("fileSet") String set, @QueryParam("path") String filePath) {

      FileSet fileSet;
      if ("lines".equals(set)) {
        fileSet = lines;
      } else if ("counts".equals(set)) {
        fileSet = counts;
      } else {
        responder.sendError(400, "Invalid file set name '" + set + "'");
        return;
      }

      Location location = fileSet.getLocation(filePath);
      ByteBuffer content;
      try {
        content = ByteBuffer.wrap(ByteStreams.toByteArray(location.getInputStream()));
      } catch (IOException e) {
        responder.sendError(400, "Unable to read path '" + filePath + "' in file set '" + set + "'");
        return;
      }
      responder.send(200, content, "application/octet-stream", ImmutableMultimap.<String, String>of());
    }

    @PUT
    @Path("{fileSet}")
    public void write(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("fileSet") String set, @QueryParam("path") String filePath) {

      FileSet fileSet;
      if ("lines".equals(set)) {
        fileSet = lines;
      } else if ("counts".equals(set)) {
        fileSet = counts;
      } else {
        responder.sendError(400, "Invalid file set name '" + set + "'");
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
        responder.sendError(400, "Unable to write path '" + filePath + "' in file set '" + set + "'");
        return;
      }
      responder.sendStatus(200);
    }
  }
}
