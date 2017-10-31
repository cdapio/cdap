/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.profiles;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A service for creating and modifying user profiles.
 */
public class UserProfileService extends AbstractService {

  private static final Gson GSON = new Gson();

  @Override
  protected void configure() {
    setName("UserProfileService");
    setDescription("A service for creating and modifying user profiles");
    addHandler(new UserProfileServiceHandler());
  }

  /**
   * Handler to create, update, and retrieve user profiles.
   */
  public class UserProfileServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("profiles")
    private Table profiles;

    @GET
    @Path("profiles")
    public void getProfiles(HttpServiceRequest request, HttpServiceResponder responder,
                            @QueryParam("users") String users,
                            @QueryParam("limit") @DefaultValue("10") int limit) {

      List<Profile> profilesList = Lists.newArrayList();

      // if the users query param is given, get the users given using a multi-get
      if (users != null && !users.isEmpty()) {
        // expecting the users param to be a comma separated list of users
        String[] usersArr = users.split("\\s*,\\s*");
        // create a list of Gets, one for each user
        List<Get> gets = Lists.newArrayListWithCapacity(usersArr.length);
        for (String user : usersArr) {
          gets.add(new Get(Bytes.toBytes(user)));
        }

        // add up to limit number of users.
        int profilesAdded = 0;
        for (Row row : profiles.get(gets)) {
          // row is empty if it could not be found.
          if (row.isEmpty()) {
            continue;
          }
          if (profilesAdded >= limit) {
            break;
          }
          profilesList.add(toProfile(row));
          profilesAdded++;
        }
      } else {
        // otherwise, scan the entire table
        try (Scanner scanner = profiles.scan(null, null)) {
          Row row;
          int profilesAdded = 0;
          while (profilesAdded < limit && (row = scanner.next()) != null) {
            profilesList.add(toProfile(row));
            profilesAdded++;
          }
        }
      }

      responder.sendJson(200, profilesList);
    }

    @GET
    @Path("profiles/{user-id}")
    public void getProfile(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("user-id") String userId) {
      Row row = profiles.get(new Get(userId));
      if (row.isEmpty()) {
        responder.sendError(404, "No such user id.");
        return;
      }
      responder.sendJson(200, toProfile(row));
    }

    @PUT
    @Path("profiles/{user-id}")
    public void createProfile(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("user-id") String userId) {
      Row row = profiles.get(new Get(userId));
      if (!row.isEmpty()) {
        responder.sendError(409, "User already exists.");
        return;
      }
      Profile profile = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), Profile.class);
      if (profile.getId() == null || profile.getName() == null || profile.getEmail() == null) {
        responder.sendError(400, "Profile must contain id, name, and email.");
        return;
      }
      if (!userId.equals(profile.getId())) {
        responder.sendError(400, "User id of profile must match user id in path.");
        return;
      }
      if (profile.getLastLogin() != null || profile.getLastActivity() != null) {
        responder.sendError(400, "Profile must not contain lastLogin or lastActivity.");
        return;
      }
      Put put = new Put(userId);
      put.add("id", userId);
      put.add("name", profile.getName());
      put.add("email", profile.getEmail());
      profiles.put(put);
      responder.sendStatus(201); // Created
    }

    @DELETE
    @Path("profiles/{user-id}")
    public void deleteProfile(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("user-id") String userId) {
      Row row = profiles.get(new Get(userId));
      if (row.isEmpty()) {
        responder.sendError(404, "No such user id.");
        return;
      }
      Delete delete = new Delete(userId);
      profiles.delete(delete);
      responder.sendStatus(200);
    }

    @PUT
    @Path("profiles/{user-id}/email")
    public void updateEmail(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("user-id") String userId) {

      String address = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      if (!address.contains("@")) {
        responder.sendError(400, "Invalid email address.");
        return;
      }
      Row row = profiles.get(new Get(userId));
      if (row.isEmpty()) {
        responder.sendError(404, "No such user id.");
        return;
      }
      Put put = new Put(userId, "email", address);
      profiles.put(put);
      responder.sendStatus(200);
    }

    @PUT
    @Path("profiles/{user-id}/lastLogin")
    public void updateLastLogin(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("user-id") String userId) {
      String body = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      long time;
      try {
        time = Long.parseLong(body);
      } catch (NumberFormatException e) {
        responder.sendError(400, "Invalid time value.");
        return;
      }
      Row row = profiles.get(new Get(userId));
      if (row.isEmpty()) {
        responder.sendError(404, "No such user id.");
        return;
      }
      Put put = new Put(userId, "login", time);
      profiles.put(put);
      responder.sendStatus(200);
    }

    private Profile toProfile(Row row) {
      String userId = Bytes.toString(row.getRow());
      String name = row.getString("name");
      String email = row.getString("email");
      Long lastLogin = row.getLong("login");
      Long lastActive = row.getLong("active");
      return new Profile(userId, name, email, lastLogin, lastActive);
    }
  }

}
