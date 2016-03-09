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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
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

    private static final Logger LOG = LoggerFactory.getLogger(FileSetHandler.class);

    @GET
    @Path("{fileSet}")
    public void read(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("fileSet") String set, @QueryParam("path") String filePath) {

      FileSet fileSet;
      try {
        fileSet = getContext().getDataset(set);
      } catch (DatasetInstantiationException e) {
        LOG.warn("Error instantiating file set {}", set, e);
        responder.sendError(400, String.format("Invalid file set name '%s'", set));
        return;
      }

      Location location = fileSet.getLocation(filePath);
      getContext().discardDataset(fileSet);

      try {
        responder.send(200, location, "application/octet-stream");
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to read path '%s' in file set '%s'", filePath, set));
      }
    }

    @PUT
    @Path("{fileSet}")
    public HttpContentConsumer write(HttpServiceRequest request, HttpServiceResponder responder,
                                     @PathParam("fileSet") final String set,
                                     @QueryParam("path") final String filePath) {

      FileSet fileSet;
      try {
        fileSet = getContext().getDataset(set);
      } catch (DatasetInstantiationException e) {
        LOG.warn("Error instantiating file set {}", set, e);
        responder.sendError(400, String.format("Invalid file set name '%s'", set));
        return null;
      }

      final Location location = fileSet.getLocation(filePath);
      getContext().discardDataset(fileSet);
      try {
        final WritableByteChannel channel = Channels.newChannel(location.getOutputStream());
        return new HttpContentConsumer() {
          @Override
          public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
            channel.write(chunk);
          }

          @Override
          public void onFinish(HttpServiceResponder responder) throws Exception {
            channel.close();
            responder.sendStatus(200);
          }

          @Override
          public void onError(HttpServiceResponder responder, Throwable failureCause) {
            Closeables.closeQuietly(channel);
            try {
              location.delete();
            } catch (IOException e) {
              LOG.warn("Failed to delete {}", location, e);
            }
            LOG.debug("Unable to write path '{}' in file set '{}'", filePath, set, failureCause);
            responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'. Reason: '%s'",
                                                   filePath, set, failureCause.getMessage()));
          }
        };
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s' in file set '%s'. Reason: '%s'",
                                               filePath, set, e.getMessage()));
        return null;
      }
    }
  }
}
