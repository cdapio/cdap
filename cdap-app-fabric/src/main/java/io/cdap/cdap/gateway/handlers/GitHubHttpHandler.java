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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.store.GitHubStore;
import io.cdap.cdap.internal.github.GitHubBranchRequest;
import io.cdap.cdap.internal.github.GitHubCheckinRequest;
import io.cdap.cdap.internal.github.GitHubCheckoutRequest;
import io.cdap.cdap.internal.github.GitHubPullRequest;
import io.cdap.cdap.internal.github.GitHubRepo;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles GitHub HTTP requests
 */
@Path(Constants.Gateway.API_VERSION_3)
public class GitHubHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private final GitHubStore gitStore;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final String keyidsuffix = "_auth_token";


  @Inject
  GitHubHttpHandler(GitHubStore gitStore, SecureStore secureStore,
      SecureStoreManager secureStoreManager) {
    this.gitStore = gitStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  /**
   * Returns all the repositories saved
   */
  @Path("repos/github")
  @GET
  public void getRepos(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepos()));
  }

  /**
   * Returns repository information for {repo}
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

  /**
   * saves {repo} repository information
   */
  @Path("repos/github/{repo}")
  @PUT
  public void addRepoInfo(FullHttpRequest request, HttpResponder responder,
                          @NotNull @PathParam("repo") String repo) throws Exception {

      GitHubRepo githubRepo = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8)
          , GitHubRepo.class);

      if (githubRepo.validateAllFields()) {
        int responseCode = testRepoConnection(githubRepo, githubRepo.getAuthString());
        if (responseCode == HttpURLConnection.HTTP_OK) {
          String authString = githubRepo.getAuthString();
          SecureKeyId authKeyId = new SecureKeyId("system", repo + keyidsuffix);
          secureStoreManager.put("system", repo + keyidsuffix, authString,
              repo + "Authorization Token", new HashMap<String, String>());
          gitStore.addOrUpdateRepo(repo, githubRepo.getUrl(), githubRepo.getDefaultBranch(),
              repo + keyidsuffix);
        }
        responder.sendString(getErrorCode(responseCode), getErrorResponse(responseCode));
      } else {
        ArrayList<String> errorFields = new ArrayList<>();
        if (!githubRepo.validNickname()) {
          errorFields.add("nickname");
        }
        if (!githubRepo.validUrl()) {
          errorFields.add("url");
        }
        if (!githubRepo.validDefaultBranch()) {
          errorFields.add("default branch");
        }
        if (!githubRepo.validAuthString()) {
          errorFields.add("authorization token");
        }
        if (!errorFields.isEmpty()) {
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "missing: " + errorFields.toString());
          return;
        }
      }
  }

  /**
   * Deletes {repo} repository information
   */
  @Path("repos/github/{repo}")
  @DELETE
  public void deleteRepoInfo(HttpRequest request, HttpResponder responder,
                              @NotNull @PathParam("repo") String repo) throws Exception {
    if (gitStore.getRepo(repo) != null) {
      gitStore.deleteRepo(repo);
      secureStoreManager.delete("system", repo + keyidsuffix);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  /**
   * tests whether the repository specified by {repo} can be accessed
   */
  @Path("repos/github/{repo}/testconnection")
  @POST
  public void testRepoConnection(HttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {
    GitHubRepo test = gitStore.getRepo(repo);
    int responseCode = testRepoConnection(test, new String(secureStore.get("system",
        repo + keyidsuffix).get(), StandardCharsets.UTF_8));
    responder.sendString(getErrorCode(responseCode), getErrorResponse(responseCode));
  }

  /**
   * Checks out a pipeline from the GitHub repository corresponding to {repo} into CDAP
   */
  @POST
  @Path("repos/github/{repo}/checkout")
  public void checkInRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    try {
      GitHubCheckoutRequest input = GSON
          .fromJson(request.content().toString(StandardCharsets.UTF_8), GitHubCheckoutRequest.class);

      String branch = !Strings.isNullOrEmpty(input.getBranch()) ?
          input.getBranch() : gitStore.getRepo(repo).getDefaultBranch();

      String path = input.getPath();

      GitHubRepo gitHubRepo = gitStore.getRepo(repo);
      URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + path + "?ref=" + branch);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      String authString = new String(secureStore.get("system", repo + keyidsuffix)
          .get(), StandardCharsets.UTF_8);
      con.setRequestProperty("Authorization", "token " + authString);

      BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));

      responder.sendString(getErrorCode(con.getResponseCode()), getErrorResponse(con.getResponseCode()));
      reader.close();
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
      return;
    }
  }

  /**
   * Checks in a pipeline from the GitHub repository corresponding to {repo} into CDAP
   */
  @POST
  @Path("repos/github/{repo}/checkin")
  public void checkOutRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    GitHubCheckinRequest pipelineInput = GSON.fromJson(request.content().
        toString(StandardCharsets.UTF_8), GitHubCheckinRequest.class);

    try {
      GitHubRepo gitHubRepo = gitStore.getRepo(repo);
    } catch (Exception ex) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
    String branch = !Strings.isNullOrEmpty(pipelineInput.getBranch()) ?
        pipelineInput.getBranch() : gitStore.getRepo(repo).getDefaultBranch();
    if (!branchExists(branch, gitStore.getRepo(repo))) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Please specify a valid branch");
    }
    GitHubRepo gitHubRepo = gitStore.getRepo(repo);
    URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + pipelineInput.getPath());
    HttpURLConnection con = (HttpURLConnection) url.openConnection();

    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system", repo + keyidsuffix)
          .get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", "token " + authString);

    JsonObject pipelineOutput = new JsonObject();

    pipelineOutput.addProperty("message", pipelineInput.getCommitMessage());
    pipelineOutput.addProperty("content", pipelineInput.getEncodedPipeline());
    pipelineOutput.addProperty("branch", branch);

    String sha = getFileSha(pipelineInput.getPath(), branch, gitHubRepo);
    if (!sha.equals("not found")) {
        pipelineOutput.addProperty("sha", sha);
    }

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(pipelineOutput.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();
    responder.sendString(getErrorCode(con.getResponseCode()), getErrorResponse(con.getResponseCode()));
  }

  /**
   * creates a branch in {repo}'s GitHub Repository
   */
  @POST
  @Path("repos/github/{repo}/createBranch")
  public void createRepoBranch(FullHttpRequest request, HttpResponder responder,
                                          @NotNull @PathParam("repo") String repo) throws Exception {

    GitHubBranchRequest branchInput = GSON.fromJson(request.content()
        .toString(StandardCharsets.UTF_8), GitHubBranchRequest.class);
    String branch = branchInput.getBranch();
    String sha = getBranchSha(gitStore.getRepo(repo).getDefaultBranch(), gitStore.getRepo(repo));

    URL url = new URL(parseUrl(gitStore.getRepo(repo).getUrl()) + "/git/refs");
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system",
        repo + keyidsuffix).get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", "token " + authString);

    JsonObject body = new JsonObject();
    body.addProperty("ref", "refs/heads/" + branch);
    body.addProperty("sha", sha);

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(body.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();

    responder.sendString(getErrorCode(con.getResponseCode()), getErrorResponse(con.getResponseCode()));
  }

  /**
   * creates a pull request from head to base (specified by user) on {repo} GitHub Repository
   */
  @POST
  @Path("repos/github/{repo}/pull")
  public void createPullRequest(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {
    GitHubRepo gitHubRepo = gitStore.getRepo(repo);
    GitHubPullRequest prInput = GSON.fromJson(request.content()
        .toString(StandardCharsets.UTF_8), GitHubPullRequest.class);
    String head = prInput.getHead();
    String base = !Strings.isNullOrEmpty(prInput.getBase()) ?
        prInput.getBase() : gitHubRepo.getDefaultBranch();
    String title = prInput.getTitle();
    String body = prInput.getBody();

    URL url = new URL(parseUrl(gitStore.getRepo(repo).getUrl()) + "/pulls");
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system",
        repo + keyidsuffix).get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", "token " + authString);

    JsonObject prOutput = new JsonObject();
    prOutput.addProperty("head", head);
    prOutput.addProperty("base", base);
    prOutput.addProperty("title", title);
    prOutput.addProperty("body", body);

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(prOutput.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();

    responder.sendString(getErrorCode(con.getResponseCode()), getErrorResponse(con.getResponseCode()));

  }

  private String parseField(String field, JsonObject jsonObject) {
    int len = jsonObject.getAsJsonPrimitive(field).toString().length();
    return jsonObject.getAsJsonPrimitive(field).toString()
        .substring(1, len - 1);
  }

  private String parseUrl(String url) throws Exception {
    URI parser = new URI(url);
    String path = parser.getPath();
    int parse = path.indexOf("/");
    String owner = path.substring(0, parse);
    String name = path.substring(parse + 1);
    return "https://api.github.com/repos" + owner + "/"
        + name;
  }

  private String retrieveContent(BufferedReader reader) throws Exception {
    String input;
    StringBuilder response = new StringBuilder();
    while ((input = reader.readLine()) != null) {
      response.append(input);
    }
    String encodedContent = parseField("content",
        GSON.fromJson(response.toString(), JsonObject.class));

    String[] contentLines = encodedContent.split("\\\\n");
    StringBuilder content = new StringBuilder();

    for (String s : contentLines) {
      content.append(new String(Base64.getDecoder().decode(s.getBytes(StandardCharsets.UTF_8)), "UTF-8"));
    }

    return content.toString();
  }

  private int testRepoConnection(GitHubRepo repo, String authString) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/branches/" + repo.getDefaultBranch());
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Authorization", "token " + authString);

    return con.getResponseCode();
  }

  private boolean branchExists(String branch, GitHubRepo repo) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/git/refs/heads/" + branch);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + keyidsuffix).get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", "token " + authString);

    return con.getResponseCode() == HttpURLConnection.HTTP_OK;
  }

  private String getBranchSha(String branch, GitHubRepo repo) throws Exception {
    URL defaultUrl = new URL(parseUrl(repo.getUrl()) + "/git/refs/heads/" + branch);
    HttpURLConnection defCon = (HttpURLConnection) defaultUrl.openConnection();
    defCon.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + keyidsuffix).get(), StandardCharsets.UTF_8);
    defCon.setRequestProperty("Authorization", "token " + authString);

    BufferedReader reader = new BufferedReader(new InputStreamReader(defCon.getInputStream()));

    String input;
    StringBuilder response = new StringBuilder();
    while ((input = reader.readLine()) != null) {
      response.append(input);
    }
    return parseField("sha", GSON.fromJson(response.toString(), JsonObject.class)
        .getAsJsonObject("object"));
  }

  private String getFileSha(String path, String branch, GitHubRepo repo) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/contents/" + path + "?ref=" + branch);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + keyidsuffix).get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", "token " + authString);

    if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));

      String input;
      StringBuilder response = new StringBuilder();
      while ((input = reader.readLine()) != null) {
        response.append(input);
      }
      return parseField("sha", GSON.fromJson(response.toString(), JsonObject.class));
    } else {
      return "not found";
    }

  }

  private HttpResponseStatus getErrorCode(int errorCode) {
    if (errorCode == HttpURLConnection.HTTP_CREATED || errorCode == HttpURLConnection.HTTP_OK) {
      return HttpResponseStatus.OK;
    } else if (errorCode == HttpURLConnection.HTTP_CONFLICT) {
      return HttpResponseStatus.CONFLICT;
    } else if (errorCode == HttpURLConnection.HTTP_FORBIDDEN) {
      return HttpResponseStatus.FORBIDDEN;
    } else if (errorCode == HttpURLConnection.HTTP_MOVED_PERM) {
      return HttpResponseStatus.MOVED_PERMANENTLY;
    } else if (errorCode == 422) {
      return HttpResponseStatus.UNAUTHORIZED;
    } else {
      return HttpResponseStatus.NOT_FOUND;
    }
  }

  private String getErrorResponse(int errorCode) {
    if (errorCode == HttpURLConnection.HTTP_CREATED || errorCode == HttpURLConnection.HTTP_OK) {
      return "Success";
    } else if (errorCode == HttpURLConnection.HTTP_CONFLICT) {
      return "This GitHub detail already exists";
    } else if (errorCode == HttpURLConnection.HTTP_FORBIDDEN) {
      return "You do not have access to this GitHub detail";
    } else if (errorCode == HttpURLConnection.HTTP_MOVED_PERM) {
      return "This GitHub detail has been moved";
    } else if (errorCode == 422) {
      return "Check your authorization token and path";
    } else {
      return "GitHub detail not found";
    }
  }
}
