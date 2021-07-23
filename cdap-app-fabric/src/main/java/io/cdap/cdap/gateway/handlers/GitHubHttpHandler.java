/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.store.GitHubStore;
import io.cdap.cdap.internal.github.GitHubRepo;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(Constants.Gateway.API_VERSION_3)
public class GitHubHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private final GitHubStore gitStore;


  @Inject
  GitHubHttpHandler(GitHubStore gitStore) {
    this.gitStore = gitStore;
  }

  @Path("repos/github")
  @GET
  public void getRepos(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepos()));
  }

  /**
   * Returns user repository information
   */
  @Path("repos/github/{repo}")
  @GET
  public void getRepoInfo(HttpRequest request, HttpResponder responder,
                          @NotNull @PathParam("repo") String repo) throws Exception {
    if (gitStore.getRepo(repo) != null) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepo(repo)));
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @Path("repos/github/{repo}")
  @PUT
  public void addRepoInfo(FullHttpRequest request, HttpResponder responder,
                          @NotNull @PathParam("repo") String repo) throws Exception {

    try {
      GitHubRepo githubRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8)
          , GitHubRepo.class);

      if (githubRequest.validateAllFields()) {
        String authString = "token " + githubRequest.getAuthString();
        gitStore.addOrUpdateRepo(repo, githubRequest.getUrl(), githubRequest.getDefaultBranch(),
            authString);
        responder.sendString(HttpResponseStatus.OK, "Repository Information Saved.");
      } else {
        String errorString = "Please enter a ";
        if (!githubRequest.validNickname()) {
          errorString += "nickname, ";
        }
        if (!githubRequest.validUrl()) {
          errorString += "url, ";
        }
        if (!githubRequest.validDefaultBranch()) {
          errorString += "default branch, ";
        }
        if (!githubRequest.validAuthString()) {
          errorString += "authorization token, ";
        }
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
            errorString.substring(0, errorString.length() - 2));
      }
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Please enter fields formatted correctly.");
    }


  }

  /**
   * Deletes current repository info
   */
  @Path("repos/github/{repo}")
  @DELETE
  public void deleteRepoInfo(HttpRequest request, HttpResponder responder,
                              @NotNull @PathParam("repo") String repo) throws Exception {
    if (gitStore.getRepo(repo) != null) {
      gitStore.deleteRepo(repo);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @Path("repos/github/{repo}/testconnection")
  @POST
  public void testRepoConnection(HttpRequest request, HttpResponder responder,
                                  @NotNull @PathParam("repo") String repo) throws Exception {
    GitHubRepo test = gitStore.getRepo(repo);
    URL url = new URL(parseUrl(test.getUrl()));
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    con.setRequestProperty("Authorization", test.getAuthString());

    if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
      responder.sendString(HttpResponseStatus.OK, "Connection Successful.");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Repository is not connectable.");
    }
  }

  public String parseUrl(String url) throws Exception {
    //Parsing SSH git url which has the format -> git@github.com:/{owner or org}/{repository name}.git
    URI parser = new URI(url);
    String path = parser.getPath();
    int parse = path.indexOf("/");
    //parse the owner and repo name
    String owner = path.substring(0, parse);
    String name = path.substring(parse + 1);
    //Put it all together
    return "https://api.github.com/repos" + owner + "/"
        + name;
  }
}
