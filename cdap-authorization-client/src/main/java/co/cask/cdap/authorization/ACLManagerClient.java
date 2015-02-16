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
package co.cask.cdap.authorization;

import co.cask.common.authorization.ACLEntry;
import co.cask.common.authorization.ACLStore;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Set;

/**
 * Provides ways to verify, create, and list ACL entries.
 */
public class ACLManagerClient {

  private static final Gson GSON = new Gson();
  private final Supplier<URI> baseURISupplier;
  private final Supplier<Multimap<String, String>> headersSupplier;

  public ACLManagerClient(Supplier<URI> baseURISupplier, Supplier<Multimap<String, String>> headersSupplier) {
    this.baseURISupplier = baseURISupplier;
    this.headersSupplier = headersSupplier;
  }

  public ACLManagerClient(Supplier<URI> baseURISupplier) {
    this(baseURISupplier, null);
  }

  public String appendQuery(String path, ACLStore.Query query) {
    List<String> arguments = Lists.newArrayList();

    if (query != null) {
      if (query.getSubjectId() != null) {
        arguments.add("subject=" + query.getSubjectId().getRep());
      }
      if (query.getObjectId() != null) {
        arguments.add("object=" + query.getObjectId().getRep());
      }
      if (query.getPermission() != null) {
        arguments.add("permission=" + query.getPermission().getName());
      }
    }

    if (!arguments.isEmpty()) {
      return path + "?" + Joiner.on("&").join(arguments);
    }

    return path;
  }

  public Set<ACLEntry> getGlobalACLs(ACLStore.Query query) throws IOException {
    String path = appendQuery("/v1/acls/global", query);
    HttpRequest request = HttpRequest.get(resolveURL(path)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<Set<ACLEntry>>() { }).getResponseObject();
  }

  public Set<ACLEntry> getACLs(String namespaceId, ACLStore.Query query) throws IOException {
    String path = appendQuery("/v1/acls/namespace/" + namespaceId, query);
    HttpRequest request = HttpRequest.get(resolveURL(path)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<Set<ACLEntry>>() { }).getResponseObject();
  }

  public Set<ACLEntry> listACLs() throws IOException {
    String path = "/v1/acls";
    HttpRequest request = HttpRequest.get(resolveURL(path)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<Set<ACLEntry>>() { }).getResponseObject();
  }

  public void deleteGlobalACLs(ACLStore.Query query) throws IOException {
    String path = appendQuery("/v1/acls/global", query);
    HttpRequest request = HttpRequest.delete(resolveURL(path)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  public void deleteACLs(String namespaceId, ACLStore.Query query) throws IOException {
    String path = appendQuery("/v1/acls/namespace/" + namespaceId, query);
    HttpRequest request = HttpRequest.delete(resolveURL(path)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  /**
   * Creates an {@link ACLEntry} in a namespace for an object, subject, and a permission.
   * This allows the subject to access the object for the specified permission.
   *
   * <p>
   * For example, if object is "secretFile", subject is "Bob", and permission is "WRITE", then "Bob"
   * would be allowed to write to the "secretFile", assuming that what is doing the writing is protecting
   * the "secretFile" via a call to one of the {@code verifyAuthorized()} or {@code isAuthorized()} calls.
   * </p>
   *
   * @param entry the {@link ACLEntry} to create
   * @throws java.io.IOException if an error occurred when contacting the authorization service
   */
  public void createACL(String namespaceId, ACLEntry entry) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v1/acls/namespace/" + namespaceId))
      .withBody(GSON.toJson(entry)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK &&
      response.getResponseCode() != HttpURLConnection.HTTP_NOT_MODIFIED) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  /**
   * Creates an {@link ACLEntry} for the global namespace, subject, and a permission.
   * This allows the subject to access the object for the specified permission.
   *
   * <p>
   * For example, if object is "secretFile", subject is "Bob", and permission is "WRITE", then "Bob"
   * would be allowed to write to the "secretFile", assuming that what is doing the writing is protecting
   * the "secretFile" via a call to one of the {@code verifyAuthorized()} or {@code isAuthorized()} calls.
   * </p>
   *
   * @param entry the {@link ACLEntry} to create
   * @throws java.io.IOException if an error occurred when contacting the authorization service
   */
  public void createGlobalACL(ACLEntry entry) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v1/acls/global"))
      .withBody(GSON.toJson(entry)).addHeaders(getHeaders()).build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK &&
      response.getResponseCode() != HttpURLConnection.HTTP_NOT_MODIFIED) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  protected URL resolveURL(String path) throws MalformedURLException {
    return baseURISupplier.get().resolve(path).toURL();
  }

  private Multimap<String, String> getHeaders() {
    return headersSupplier == null ? HashMultimap.<String, String>create() : headersSupplier.get();
  }
}
