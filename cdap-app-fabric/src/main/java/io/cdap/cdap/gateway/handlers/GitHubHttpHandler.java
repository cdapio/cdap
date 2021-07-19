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
import java.util.Base64;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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

  @Path("repos/githt")
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
                          @PathParam("repo") String repo) throws Exception {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepo(repo)));
  }

  @Path("repos/github/{repo}")
  @PUT
  public void addRepoInfo(FullHttpRequest request, HttpResponder responder,
                          @PathParam("repo") String repo) throws Exception {

    GitHubRepo githubRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8)
        , GitHubRepo.class);

    String htmlUrl =  parseUrl(githubRequest.getUrl());

    String authString  = "invalid";
    if (githubRequest.getAuthMethod().equals("username and password")) {
      authString = "Basic " + Base64.getEncoder().encodeToString(
            (githubRequest.getUsername() + ":" + githubRequest.getPassword()).getBytes());
    } else if (githubRequest.getAuthMethod().equals("authorization token")) {
      authString = "token " + githubRequest.getAuthToken();
    }
    //test that the GitHub repository information is valid
    if (checkGitConnection(htmlUrl, authString)) {
      String defaultBranch = githubRequest.getDefaultBranch();
      gitStore.addOrUpdateRepo(repo, githubRequest.getUrl(), githubRequest.getDefaultBranch(),
            githubRequest.getAuthMethod(), githubRequest.getUsername(),
          githubRequest.getPassword(), githubRequest.getAuthToken());
      responder.sendStatus(HttpResponseStatus.OK);
      }
  }

  /**
   * Deletes current repository info
   */
  @Path("repos/github/{repo}")
  @DELETE
  public void deleteRepoInfo(HttpRequest request, HttpResponder responder,
                              @PathParam("repo") String repo) throws Exception {
    gitStore.deleteRepo(repo);
    responder.sendStatus(HttpResponseStatus.OK);
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

  public boolean checkGitConnection(String htmlUrl, String authString) throws Exception {
    URL url = new URL(htmlUrl);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Authorization", authString);
    return con.getResponseCode() == HttpURLConnection.HTTP_OK;
  }
}
