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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.profile.ProvisionerInfo;
import co.cask.cdap.proto.profile.ProvisionerPropertyValue;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
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
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProfileHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();
  // TODO: remove these variables
  private static final List<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableList.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();
  private static final Profile SYSTEM_PROFILE = new Profile("system mock", "system mock profile for UI",
                                                            EntityScope.SYSTEM,
                                                            new ProvisionerInfo("mock provisioner",
                                                                                PROPERTY_SUMMARIES));
  private static final Profile USER_PROFILE = new Profile("user mock", "user mock profile for UI",
                                                            EntityScope.USER,
                                                            new ProvisionerInfo("mock provisioner",
                                                                                PROPERTY_SUMMARIES));


  /**
   * List the profiles in the given namespace. By default the results will not contain profiles in system scope.
   */
  @GET
  @Path("/profiles")
  public void getProfiles(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @Nullable @QueryParam("includeSystem") String includeSystem) {
    // TODO: implement the method and remove the mock data
    List<Profile> profiles = new ArrayList<>();
    profiles.add(SYSTEM_PROFILE);
    profiles.add(USER_PROFILE);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profiles));
  }

  /**
   * Get the information about a specific profile.
   */
  @GET
  @Path("/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("profile-name") String profileName) throws NotFoundException {
    // TODO: implement the method and remove the mock data
    if (new NamespaceId(namespaceId).equals(NamespaceId.SYSTEM)) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(SYSTEM_PROFILE));
    } else {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(USER_PROFILE));
    }
  }

  /**
   * Write a profile in a namespace.
   */
  @PUT
  @Path("/profiles/{profile-name}")
  public void writeProfile(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("profile-name") String profileName) throws BadRequestException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete a profile from a namespace. A profile must be in the disabled state before it can be deleted.
   * Before a profile can be deleted, it cannot be assigned to any program or schedule,
   * and it cannot be in use by any running program.
   */
  @DELETE
  @Path("/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws NotFoundException, ConflictException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Disable the profile, so that no new program runs can use it,
   * and no new schedules/programs can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/disable")
  public void disableProfile(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("profile-name") String profileName) throws NotFoundException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Enable the profile, so that programs/schedules can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/enable")
  public void enableProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws NotFoundException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
