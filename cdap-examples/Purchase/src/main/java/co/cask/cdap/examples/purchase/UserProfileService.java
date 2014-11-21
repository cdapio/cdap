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

package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffers;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class UserProfileService extends AbstractService {
  public static final String SERVICE_NAME = "UserProfileService";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A Service to update and retrieve user profiles.");
    addHandler(new UserProfileServiceHandler());
  }

  /**
   *
   */
  public static final class UserProfileServiceHandler extends AbstractHttpServiceHandler {

    private static final Gson GSON = new Gson();

    @UseDataSet("userProfiles")
    private KeyValueTable userProfiles;

    @Path("user/{id}")
    @GET
    public void getUserProfile(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
      byte[] encodedUserProfile = userProfiles.read(id);
      if (encodedUserProfile == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No profile found for user : %s", id), Charsets.UTF_8);
      } else {
        UserProfile userProfile = GSON.fromJson(Bytes.toString(encodedUserProfile), UserProfile.class);
        responder.sendJson(userProfile);
      }
    }

    @Path("user")
    @POST
    public void setUserProfile(HttpServiceRequest request, HttpServiceResponder responder) {
      try {
        String encodedUserProfile = new String(ChannelBuffers.copiedBuffer(request.getContent()).array());
        UserProfile userProfile = GSON.fromJson(encodedUserProfile, UserProfile.class);
        userProfiles.write(userProfile.getId(), GSON.toJson(userProfile));
        responder.sendStatus(HttpURLConnection.HTTP_OK);
      } catch (Exception e) {
        responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST, "Could not decode user profile.", Charsets.UTF_8);
      }
    }
  }
}
