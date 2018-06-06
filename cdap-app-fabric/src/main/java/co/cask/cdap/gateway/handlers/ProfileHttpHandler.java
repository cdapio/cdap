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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.profile.ProfileCreateRequest;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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

  private final ProfileService profileService;
  private final ProvisioningService provisioningService;

  @Inject
  public ProfileHttpHandler(ProfileService profileService, ProvisioningService provisioningService) {
    this.profileService = profileService;
    this.provisioningService = provisioningService;
  }

  /**
   * List the profiles in the given namespace. By default the results will not contain profiles in system scope.
   */
  @GET
  @Path("/profiles")
  public void getProfiles(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @QueryParam("includeSystem") @DefaultValue("false") String includeSystem) {
    NamespaceId namespace = new NamespaceId(namespaceId);
    boolean include = Boolean.valueOf(includeSystem);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfiles(namespace, include)));
  }

  /**
   * Get the information about a specific profile.
   */
  @GET
  @Path("/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("profile-name") String profileName) throws NotFoundException {
    ProfileId profileId = new ProfileId(namespaceId, profileName);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileService.getProfile(profileId)));
  }

  /**
   * Write a profile in a namespace.
   */
  @PUT
  @Path("/profiles/{profile-name}")
  public void writeProfile(FullHttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("profile-name") String profileName) throws BadRequestException, IOException {
    ProfileCreateRequest profileCreateRequest;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      profileCreateRequest = GSON.fromJson(reader, ProfileCreateRequest.class);
      validateProvisionerProperties(profileCreateRequest);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Unable to parse request body. Please make sure it is valid JSON", e);
    }
    ProfileId profileId = new ProfileId(namespaceId, profileName);
    Profile profile =
      new Profile(profileName, profileCreateRequest.getDescription(),
                  profileId.getNamespaceId().equals(NamespaceId.SYSTEM) ? EntityScope.SYSTEM : EntityScope.USER,
                  profileCreateRequest.getProvisioner());
    profileService.saveProfile(profileId, profile);
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
                            @PathParam("profile-name") String profileName)
    throws NotFoundException, ProfileConflictException {
    // TODO: add the check if there is any program or schedule associated with the profile
    profileService.deleteProfile(new ProfileId(namespaceId, profileName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/profiles/{profile-name}/status")
  public void getProfileStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("profile-name") String profileName) throws NotFoundException {
    responder.sendJson(HttpResponseStatus.OK,
      GSON.toJson(profileService.getProfile(new ProfileId(namespaceId, profileName)).getStatus()));
  }

  /**
   * Disable the profile, so that no new program runs can use it,
   * and no new schedules/programs can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/disable")
  public void disableProfile(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("profile-name") String profileName)
    throws NotFoundException, ProfileConflictException {
    profileService.disableProfile(new ProfileId(namespaceId, profileName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Enable the profile, so that programs/schedules can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/enable")
  public void enableProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName)
    throws NotFoundException, ProfileConflictException {
    profileService.enableProfile(new ProfileId(namespaceId, profileName));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void validateProvisionerProperties(ProfileCreateRequest request) throws BadRequestException {
    ProvisionerInfo provisionerInfo = request.getProvisioner();
    // this will only happen when the json file is valid, but contains no provisioner fields, GSON will serialize these
    // fields with null, so accessing it will get a NullPointerException
    if (provisionerInfo == null || provisionerInfo.getName() == null) {
      throw new BadRequestException("Missing provisioner information in the json file. " +
                                      "A profile must be associated with a provisioner.");
    }
    Map<String, String> properties = new HashMap<>();
    Collection<ProvisionerPropertyValue> provisionerProperties = provisionerInfo.getProperties();
    if (provisionerProperties != null) {
      for (ProvisionerPropertyValue value : provisionerProperties) {
        if (value == null) {
          continue;
        }
        properties.put(value.getName(), value.getValue());
      }
    }
    try {
      provisioningService.validateProperties(provisionerInfo.getName(), properties);
    } catch (NotFoundException e) {
      throw new BadRequestException(String.format("The specified provisioner %s does not exist, " +
                                                    "thus cannot be associated with a profile",
                                                  provisionerInfo.getName()), e);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage(), e);
    }
  }
}
