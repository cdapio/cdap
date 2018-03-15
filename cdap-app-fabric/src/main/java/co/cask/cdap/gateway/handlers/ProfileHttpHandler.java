/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;

import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} for managing profiles.
 */
public class ProfileHttpHandler extends AbstractHttpHandler {

  /**
   * List the profiles in the given namespace. By default the results will not contain profiles in system scope.
   */
  @GET
  @Path("/namespaces/{namespace-id}/profiles")
  public void getProfiles(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @Nullable @QueryParam("includeSystem") String includeSystem) {
    // TODO: implement the method
  }

  /**
   * Get the information about a specific profile.
   */
  @GET
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("name") String profileName) throws NotFoundException {
    // TODO: implement the method
  }

  /**
   * Write a profile in a namespace.
   */
  @PUT
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void writeProfile(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("name") String profileName) throws BadRequestException {
    // TODO: implement the method
  }

  /**
   * Delete a profile from a namespace. A profile must be in the disabled state before it can be deleted.
   * Before a profile can be deleted, it cannot be assigned to any program or schedule,
   * and it cannot be in use by any running program.
   */
  @DELETE
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String profileName) throws NotFoundException, ConflictException {
    // TODO: implement the method
  }

  /**
   * Disable the profile, so that no new program runs can use it,
   * and no new schedules/programs can be assigned to it.
   */
  @POST
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}/disable")
  public void disableProfile(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("name") String profileName) throws NotFoundException {
    // TODO: implement the method
  }

  /**
   * Enable the profile, so that programs/schedules can be assigned to it.
   */
  @POST
  @Path("/namespaces/{namespace-id}/profiles/{profile-name}/disable")
  public void enableProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String profileName) throws NotFoundException {
    // TODO: implement the method
  }
}
