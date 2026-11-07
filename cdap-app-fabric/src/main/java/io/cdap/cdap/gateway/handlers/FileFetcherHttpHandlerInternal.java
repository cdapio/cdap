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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.LocationBodyProducer;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Internal {@link HttpHandler} for fetching file from {@link LocationFactory}
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class FileFetcherHttpHandlerInternal extends AbstractHttpHandler {

  private final LocationFactory locationFactory;
  private final ContextAccessEnforcer contextAccessEnforcer;

  @Inject
  FileFetcherHttpHandlerInternal(LocationFactory locationFactory,
      ContextAccessEnforcer contextAccessEnforcer) {
    this.locationFactory = locationFactory;
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  /**
   * Download the file specified by the given path in the URL.
   *
   * @param request {@link HttpRequest}
   * @param responder {@link HttpResponse}
   */
  @GET
  @Path("/location/**")
  public void download(HttpRequest request, HttpResponder responder) throws Exception {
    // This handler resolves the requested path as an absolute location against the
    // whole storage backend (see Locations#getLocationFromAbsolutePath), not scoped to
    // any namespace or caller-owned resource, so it must be restricted to callers with
    // instance-level access, same as the other raw-internal-state handler, ConfigHandler.
    contextAccessEnforcer.enforce(InstanceId.SELF, StandardPermission.GET);
    String prefix = String.format("%s/location/", Constants.Gateway.INTERNAL_API_VERSION_3);
    String path = request.uri().substring(prefix.length());
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);

    if (!location.exists()) {
      throw new NotFoundException(String.format("Path %s is not found", path));
    }
    responder.sendContent(HttpResponseStatus.OK, new LocationBodyProducer(location),
        new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
  }
}
