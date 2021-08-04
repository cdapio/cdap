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
import io.cdap.cdap.internal.github.GitHubIO;
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
import java.util.Base64;
import java.util.HashMap;
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

  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;


  @Inject
  GitHubHttpHandler(GitHubStore gitStore, SecureStore secureStore,
      SecureStoreManager secureStoreManager) {
    this.gitStore = gitStore;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
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

      GitHubRepo githubRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8)
          , GitHubRepo.class);

      if (githubRequest.validateAllFields()) {
        //test the repo's connection to ensure it exists.
        int responseCode = testRepoConnection(githubRequest,
            "token " + githubRequest.getAuthString());
        if (responseCode == HttpURLConnection.HTTP_OK) {
          String authString = "token " + githubRequest.getAuthString();
          SecureKeyId authKeyId = new SecureKeyId("system", repo + "_auth_token");
          secureStoreManager.put("system", repo + "_auth_token", authString,
              repo + "Authorization Token", new HashMap<String, String>());
          gitStore.addOrUpdateRepo(repo, githubRequest.getUrl(), githubRequest.getDefaultBranch(),
              repo + "_auth_token");
          responder.sendString(HttpResponseStatus.OK, "Repository Information Saved.");
        } else if (responseCode == HttpURLConnection.HTTP_MOVED_PERM) {
          responder.sendString(HttpResponseStatus.MOVED_PERMANENTLY, "Repository has been moved.");
        } else if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
          responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this repository.");
        } else {
          responder.sendString(HttpResponseStatus.NOT_FOUND, "Repository was not found.");
        }
        //If the user leaves fields blank, throw back which fields are missing
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
      secureStoreManager.delete("system", repo + "_auth_token");
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
    int responseCode = testRepoConnection(test, new String(secureStore.get("system",
        repo + "_auth_token").get(), StandardCharsets.UTF_8));
    if (responseCode == HttpURLConnection.HTTP_OK) {
      responder.sendString(HttpResponseStatus.OK, "Connection Successful.");
    } else if (responseCode == HttpURLConnection.HTTP_MOVED_PERM) {
      responder.sendString(HttpResponseStatus.MOVED_PERMANENTLY, "Repository has been moved.");
    } else if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this repository.");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Repository was not found.");
    }
  }

  @POST
  @Path("repos/github/import/{repo}")
  public void checkInRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    try {
      GitHubIO input = GSON
          .fromJson(request.content().toString(StandardCharsets.UTF_8), GitHubIO.class);

      String branch = gitStore.getRepo(repo).getDefaultBranch();
      if (!Strings.isNullOrEmpty(input.getBranch())) {
        branch = input.getBranch();
      }

      String path = input.getPath();

      GitHubRepo gitHubRepo = gitStore.getRepo(repo);
      URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + path + "?ref=" + branch);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      String authString = new String(secureStore.get("system", repo + "_auth_token")
          .get(), StandardCharsets.UTF_8);
      con.setRequestProperty("Authorization", authString);

      BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));

      if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
        responder.sendString(HttpResponseStatus.OK, retrieveContent(reader));
      } else if (con.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this file");
      } else {
        responder
            .sendString(HttpResponseStatus.NOT_FOUND, "File not found, check filepath + branch.");
      }
      reader.close();
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Verify that file exists.");
    }
  }

  @POST
  @Path("repos/github/export/{repo}")
  public void checkOutRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    GitHubIO pipelineInput = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), GitHubIO.class);

    String branch = gitStore.getRepo(repo).getDefaultBranch();
    if (!Strings.isNullOrEmpty(pipelineInput.getBranch())) {
      branch = pipelineInput.getBranch();
      if (!branchExists(branch, gitStore.getRepo(repo))) {
        responder.sendString(HttpResponseStatus.NOT_FOUND, "Please specify a valid branch");
      }
    }

    GitHubRepo gitHubRepo = gitStore.getRepo(repo);

    URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + pipelineInput.getPath());
    HttpURLConnection con = (HttpURLConnection) url.openConnection();

    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system", repo + "_auth_token")
          .get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", authString);

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

    if (con.getResponseCode() == HttpURLConnection.HTTP_CREATED ||
          con.getResponseCode() == HttpURLConnection.HTTP_OK) {
        responder.sendString(HttpResponseStatus.OK, "File exported.");
    } else if (con.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
        responder.sendString(HttpResponseStatus.CONFLICT, "This file already exists, "
            + "provide a sha in order to update it");
    } else if (con.getResponseCode() == 422) {
        responder.sendString(HttpResponseStatus.UNAUTHORIZED,
            "Please check your authorization key and file path \n" + pipelineOutput.toString());
    } else {
        responder.sendString(HttpResponseStatus.NOT_FOUND, con.getResponseCode() +
            " file destination not found ");
    }
  }

  @POST
  @Path("repos/github/createBranch/{repo}")
  public void createRepoBranch(FullHttpRequest request, HttpResponder responder,
                                          @NotNull @PathParam("repo") String repo) throws Exception {

    GitHubIO branchInput = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), GitHubIO.class);
    String branch = branchInput.getBranch();
    String sha = getBranchSha(gitStore.getRepo(repo).getDefaultBranch(), gitStore.getRepo(repo));

    URL url = new URL(parseUrl(gitStore.getRepo(repo).getUrl()) + "/git/refs");
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system",
        repo + "_auth_token").get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", authString);

    JsonObject body = new JsonObject();
    body.addProperty("ref", "refs/heads/" + branch);
    body.addProperty("sha", sha);

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(body.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();

    if (con.getResponseCode() == HttpURLConnection.HTTP_CREATED ||
        con.getResponseCode() == HttpURLConnection.HTTP_OK) {
      responder.sendString(HttpResponseStatus.OK, "Branch created.");
    } else if (con.getResponseCode() == 422) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          "Please check your authorization key and file path");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, con.getResponseCode() +
          " branch destination not found ");
    }
  }

  @POST
  @Path("repos/github/pull/{repo}")
  public void createPullRequest(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {
    GitHubRepo gitHubRepo = gitStore.getRepo(repo);
    JsonObject prInput = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);
    String head = parseField("head", prInput);
    String base = gitHubRepo.getDefaultBranch();
    if (prInput.has("base") && !Strings.isNullOrEmpty(parseField("base", prInput))) {
      base = parseField("base", prInput);
    }
    String title = parseField("title", prInput);
    String body = parseField("body", prInput);

    URL url = new URL(parseUrl(gitStore.getRepo(repo).getUrl()) + "/pulls");
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    String authString = new String(secureStore.get("system",
        repo + "_auth_token").get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", authString);

    JsonObject prOutput = new JsonObject();
    prOutput.addProperty("head", head);
    prOutput.addProperty("base", base);
    prOutput.addProperty("title", title);
    prOutput.addProperty("body", body);

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(prOutput.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();

    if (con.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
      responder.sendString(HttpResponseStatus.OK, "Pull Request created.");
    } else if (con.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          "Please verify your auth token.");
    } else if (con.getResponseCode() == 422) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          "Please check your parameters");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, con.getResponseCode() +
          " pr destination not found ");
    }

  }

  public String parseField(String field, JsonObject jsonObject) {
    int len = jsonObject.getAsJsonPrimitive(field).toString().length();
    return jsonObject.getAsJsonPrimitive(field).toString()
        .substring(1, len - 1);
  }

  public String parseUrl(String url) throws Exception {
    //Parsing SSH git url which has the format -> https://github.com/{owner or org}/{repository name}
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

  public String retrieveContent(BufferedReader reader) throws Exception {
    String input;
    StringBuilder response = new StringBuilder();
    while ((input = reader.readLine()) != null) {
      response.append(input);
    }
    //get the actual content or JSON pipeline
    String encodedContent = parseField("content",
        GSON.fromJson(response.toString(), JsonObject.class));

    String[] contentLines = encodedContent.split("\\\\n");
    StringBuilder content = new StringBuilder();

    for (String s : contentLines) {
      content.append(new String(Base64.getDecoder().decode(s.getBytes(StandardCharsets.UTF_8)), "UTF-8"));
    }

    return content.toString();
  }

  public int testRepoConnection(GitHubRepo repo, String authString) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/branches/" + repo.getDefaultBranch());
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Authorization", authString);

    return con.getResponseCode();
  }

  public boolean branchExists(String branch, GitHubRepo repo) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/git/refs/heads/" + branch);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + "_auth_token").get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", authString);

    return con.getResponseCode() == HttpURLConnection.HTTP_OK;
  }

  public String getBranchSha(String branch, GitHubRepo repo) throws Exception {
    URL defaultUrl = new URL(parseUrl(repo.getUrl()) + "/git/refs/heads/" + branch);
    HttpURLConnection defCon = (HttpURLConnection) defaultUrl.openConnection();
    defCon.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + "_auth_token").get(), StandardCharsets.UTF_8);
    defCon.setRequestProperty("Authorization", authString);

    BufferedReader reader = new BufferedReader(new InputStreamReader(defCon.getInputStream()));

    String input;
    StringBuilder response = new StringBuilder();
    while ((input = reader.readLine()) != null) {
      response.append(input);
    }
    return parseField("sha", GSON.fromJson(response.toString(), JsonObject.class)
        .getAsJsonObject("object"));
  }

  public String getFileSha(String path, String branch, GitHubRepo repo) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()) + "/contents/" + path + "?ref=" + branch);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    String authString = new String(secureStore.get("system",
        repo.getNickname() + "_auth_token").get(), StandardCharsets.UTF_8);
    con.setRequestProperty("Authorization", authString);

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
}
