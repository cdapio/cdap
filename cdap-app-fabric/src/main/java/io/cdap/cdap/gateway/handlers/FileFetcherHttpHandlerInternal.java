/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.InputStream;
import java.net.URI;
import java.util.Base64;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Internal {@link HttpHandler} for serving file downloading requests.
 *
 * This is currently used by FileLocalizer to download required files from AppFabric
 * in order to launch TwillApplication.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class FileFetcherHttpHandlerInternal extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();

  private final LocationFactory locationFactory;

  @Inject
  FileFetcherHttpHandlerInternal(CConfiguration configuration,
                                 LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  /**
   * Download the file specified by the base64-encoded URI
   *
   * @param request   {@link HttpRequest}
   * @param responder {@link HttpResponse}
   * @param encodedUri base64 encoded URI that identifies the target file to download.
   * @throws Exception if the file is not found or downloading fails.
   */
  @GET
  @Path("/location/{encodedUri}")
  public void download(HttpRequest request, HttpResponder responder,
                          @PathParam("encodedUri") String encodedUri) throws Exception {
    URI uri = new URI(new String(Base64.getDecoder().decode(encodedUri)));
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, uri.getPath());
    InputStream inputStream = location.getInputStream();
    byte[] buf = new byte[32 * 1024];
    try {
      responder.sendContent(HttpResponseStatus.OK, new BodyProducer() {
        @Override
        public ByteBuf nextChunk() throws Exception {
          int len = inputStream.read(buf);
          if (len == -1) {
            return Unpooled.EMPTY_BUFFER;
          }
          return Unpooled.copiedBuffer(buf, 0, len);
        }
        @Override
        public void finished() throws Exception {
          Closeables.closeQuietly(inputStream);
        }

        @Override
        public void handleError(@Nullable Throwable cause) {
          Closeables.closeQuietly(inputStream);
        }
      }, EmptyHttpHeaders.INSTANCE);
    } catch (Exception e) {
      Closeables.closeQuietly(inputStream);
      throw e;
    }
  }
}
