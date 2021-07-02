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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.store.NamespaceStore;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Program Preferences HTTP Handler.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class PreferencesHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Gson DECODE_GSON = new GsonBuilder()
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
      .create();

  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final PreferencesService preferencesService;
  private final Store store;
  private final NamespaceStore nsStore;
  private JsonObject userRepo = new JsonObject();

  @Inject
  PreferencesHttpHandler(AccessEnforcer accessEnforcer,
                         AuthenticationContext authenticationContext,
                         PreferencesService preferencesService,
                         Store store, NamespaceStore nsStore) {
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.preferencesService = preferencesService;
    this.store = store;
    this.nsStore = nsStore;
  }

  /**
   * Returns user repository information
   */
  @GET
  @Path("git/repo/info")
  public void getRepoInfo(HttpRequest request, HttpResponder responder) throws Exception {
    //check if repo info exists
    if (userRepo.toString().equals("{}")) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, GSON.toJson("Please add repo information first."));
    } else {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(userRepo));
    }
  }

  /**
   * Saves and validates repository information from user
   */
  @PUT
  @Path("git/repo/add")
  public void addRepoInfo(FullHttpRequest request, HttpResponder responder) throws Exception {
    //first check if repository info has already been saved
    if (!userRepo.toString().equals("{}")) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, GSON.toJson("Repo Information is already saved"));
    } else {
      JsonObject jObj = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);
      //check that the Json has the necessary fields
      if (jObj.has("owner") && jObj.has("repoName")) {
        String owner = jObj.getAsJsonPrimitive("owner").toString();
        String repo = jObj.getAsJsonPrimitive("repoName").toString();
        //connect to GitHub
        String htmlUrl =  "https://api.github.com/repos/" + owner.substring(1, owner.length() - 1) + "/"
            + repo.substring(1, repo.length() - 1);
        URL url = new URL(htmlUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        //test that the GitHub repository information is valid
        if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
          userRepo = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);
          responder.sendJson(HttpResponseStatus.OK, GSON.toJson("Repository Info has been saved. "));
        } else {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST, "This repository does not exist.");
        }
      } else {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Enter fields correctly.");
      }
    }
  }
  /*@GET
  @Path("/git/repos/{owner}/{repo}")
  public void getRepo(HttpRequest request, HttpResponder responder,
      @PathParam("owner") String owner, @PathParam("repo") String repo)
      throws Exception {
    URL obj = new URL("https://api.github.com/repos/" + owner + "/" + repo);
    HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
    conn.setRequestMethod("GET");
    int responseCode = conn.getResponseCode();
    StringBuilder output = new StringBuilder("failed");
    StringBuilder header = new StringBuilder();
    ArrayList<String> info = new ArrayList<>();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      BufferedReader in  = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      String inp;
      output = new StringBuilder();
      info.add("Owner : " + owner);
      info.add("Repo Name : " + repo);
      header.append("Owner : " + owner + "\n\n");
      while ((inp = in.readLine()) != null) {
        output.append(inp);
      }
      String[] parameters = output.toString().split(",");
      output = new StringBuilder();
      Boolean urlCheck = false;
      for (String s : info) {
        output.append(s + "\n");
      }
      for (String s : parameters) {
        //Find description
        if (s.contains("description")) {
          output.append("Description : " + s.substring("xxxdescription:".length(), s.length() - 1) + "\n");
        }
        //Find default branch
        if (s.contains("default_branch")) {
          output.append("Default Branch : " + s.substring("xxxdefault_branch:".length(),
              s.length() - 1) + "\n");
        }
        //Find username
        if (s.contains("login")) {
          output.append("Username : " + s.substring("xxxowner:{login:xx".length(), s.length() - 1) + "\n");
        }
        if (s.contains("html_url") && !urlCheck) {
          urlCheck = true;
        } else if (s.contains("html_url") && urlCheck) {
          output.append("URL : " + s.substring("xxxhtml_url:".length(), s.length() - 1) + "\n");
        }
        header.append(s + "\n");
      }

      in.close();
    }

    //header.append(output.toString());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(output.toString()));

  }

  @Path("/git/user/repo/{token}")
  @POST
  public void createGitRepo(FullHttpRequest request, HttpResponder responder,
                            @PathParam("token") String token) throws Exception {
    URL obj = new URL("https://api.github.com/user/repos");
    HttpURLConnection con  = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    con.setRequestProperty("Authorization", "token " + token);
    JsonObject jObj = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);
    String sampleInput = "{\"name\":\"new-repo-2\",\"description\":\"sample repo\"}";
    byte[] info = jObj.toString().getBytes(
        StandardCharsets.UTF_8);
    DataOutputStream wout = new DataOutputStream(con.getOutputStream());
    wout.write(info);
    wout.flush();
    wout.close();

    int responseCode = con.getResponseCode();
    BufferedReader inp = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String in;
    StringBuilder response = new StringBuilder();
    while ((in = inp.readLine()) != null) {
      response.append(in);
    }
    inp.close();


    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responseCode + "\n" +
        response.toString() + "\n" + jObj.toString()));
  }

  @PUT
  @Path("/git/{owner}/{repo}/{token}/{location}")
  public void sendFile(FullHttpRequest request, HttpResponder responder,
                       @PathParam("owner") String owner, @PathParam("repo") String repo,
                       @PathParam("token") String token, @PathParam("location") String location) throws Exception {
    JsonObject jObj = DECODE_GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);
    String message = jObj.get("message").toString();
    //String filepath = jObj.get("file path").toString();
    String content = jObj.get("content").toString();
    location.replace('-', '/');
    String url = "https://api.github.com/repos/" + owner + "/" + repo + "/contents/" + location;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    con.setRequestProperty("Authorization", "token " + token);

    DataOutputStream wout = new DataOutputStream(con.getOutputStream());
    //Encode contents of file
    /*File file = new File(content);
    FileInputStream inStream = new FileInputStream(file);
    byte[] bytes = new byte[(int) file.length()];
    inStream.read(bytes);
    String encode = Base64.getEncoder().encodeToString(bytes);
    JsonObject jOut = new JsonObject();
    jOut.addProperty("message", message);
    jOut.addProperty("content", content);
    byte[] info = jObj.toString().getBytes(
        StandardCharsets.UTF_8);
    wout.write(info);
    wout.flush();
    wout.close();
    int responseCode = con.getResponseCode();
    BufferedReader inp = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String in;
    StringBuilder response = new StringBuilder();
    while ((in = inp.readLine()) != null) {
      response.append(in);
    }
    inp.close();


    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responseCode + "\n" +
        response.toString() + "\n" + jOut.toString()));
  }*/
  //Instance Level Properties
  @Path("/preferences")
  @GET
  public void getInstancePrefs(HttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    accessEnforcer.enforce(instanceId, authenticationContext.getPrincipal(), StandardPermission.GET);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties()));
  }

  @Path("/preferences")
  @DELETE
  public void deleteInstancePrefs(HttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    accessEnforcer.enforce(instanceId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    preferencesService.deleteProperties();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setInstancePrefs(FullHttpRequest request, HttpResponder responder) throws Exception {
    InstanceId instanceId = new InstanceId("");
    accessEnforcer.enforce(instanceId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  //Namespace Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace)
  @Path("/namespaces/{namespace-id}/preferences")
  @GET
  public void getNamespacePrefs(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespace, @QueryParam("resolved") boolean resolved)
    throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), StandardPermission.GET);
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      if (resolved) {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(preferencesService.getResolvedProperties(namespaceId)));
      } else {
        responder.sendJson(HttpResponseStatus.OK,
                           GSON.toJson(preferencesService.getProperties(namespaceId)));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void setNamespacePrefs(FullHttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(namespaceId, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/preferences")
  @DELETE
  public void deleteNamespacePrefs(HttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (nsStore.get(namespaceId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Namespace %s not present", namespace));
    } else {
      preferencesService.deleteProperties(namespaceId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Application Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace < Application)
  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @GET
  public void getAppPrefs(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                          @QueryParam("resolved") boolean resolved) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    accessEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), StandardPermission.GET);
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      if (resolved) {
        responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getResolvedProperties(applicationId)));
      } else {
        responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties(applicationId)));
      }
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void putAppPrefs(FullHttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    accessEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
      return;
    }

    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(applicationId, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/preferences")
  @DELETE
  public void deleteAppPrefs(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId)
    throws Exception {
    ApplicationId applicationId = new ApplicationId(namespace, appId);
    accessEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (store.getApplication(applicationId) == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, String.format("Application %s in Namespace %s not present",
                                                                       appId, namespace));
    } else {
      preferencesService.deleteProperties(applicationId);
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }

  //Program Level Properties
  //Resolved field, if set to true, returns the collapsed property map (Instance < Namespace < Application < Program)
  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @GET
  public void getProgramPrefs(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                              @PathParam("program-type") String programType, @PathParam("program-id") String programId,
                              @QueryParam("resolved") boolean resolved) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    accessEnforcer.enforce(program, authenticationContext.getPrincipal(), StandardPermission.GET);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    if (resolved) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getResolvedProperties(program)));
    } else {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(preferencesService.getProperties(program)));
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @PUT
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void putProgramPrefs(FullHttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespace,
                              @PathParam("application-id") String appId,
                              @PathParam("program-type") String programType,
                              @PathParam("program-id") String programId) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    accessEnforcer.enforce(program, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    try {
      Map<String, String> propMap = decodeArguments(request);
      preferencesService.setProperties(program, propMap);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (JsonSyntaxException jsonEx) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid JSON in body");
    }
  }

  @Path("/namespaces/{namespace-id}/apps/{application-id}/{program-type}/{program-id}/preferences")
  @DELETE
  public void deleteProgramPrefs(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespace, @PathParam("application-id") String appId,
                                 @PathParam("program-type") String programType,
                                 @PathParam("program-id") String programId) throws Exception {
    ProgramId program = new ProgramId(namespace, appId, getProgramType(programType), programId);
    accessEnforcer.enforce(program, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    Store.ensureProgramExists(program, store.getApplication(program.getParent()));
    preferencesService.deleteProperties(program);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Parses the give program type into {@link ProgramType} object.
   *
   * @param programType the program type to parse.
   * @throws BadRequestException if the given program type is not a valid {@link ProgramType}.
   */
  private ProgramType getProgramType(String programType) throws BadRequestException {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Invalid program type '%s'", programType), e);
    }
  }
}
