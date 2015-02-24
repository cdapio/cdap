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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Set;

/**
 * Provides ways to verify, create, and list ACL entries.
 */
public class ACLManagerClient {

  private static final Gson GSON = new Gson();
  private static final Type ACL_SET_TYPE = new TypeToken<Set<ACLEntry>>() { }.getType();

  private final Supplier<URI> baseURISupplier;
  private final Supplier<Multimap<String, String>> headersSupplier;

  public ACLManagerClient(Supplier<URI> baseURISupplier, Supplier<Multimap<String, String>> headersSupplier) {
    this.baseURISupplier = baseURISupplier;
    this.headersSupplier = headersSupplier;
  }

  public ACLManagerClient(Supplier<URI> baseURISupplier) {
    this(baseURISupplier, null);
  }

  /**
   * TODO
   *
   * @param query
   * @return
   * @throws IOException
   */
  public Set<ACLEntry> searchACLs(ACLStore.Query query) throws IOException {
    HttpRequest request = HttpRequest.get(resolveURL("/v3/acls/search"))
      .withBody(GSON.toJson(query))
      .addHeaders(getHeaders())
      .build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.<Set<ACLEntry>>fromJsonBody(response, ACL_SET_TYPE).getResponseObject();
  }

  /**
   * TODO
   *
   * @return
   * @throws IOException
   */
  public Set<ACLEntry> getACLs() throws IOException {
    HttpRequest request = HttpRequest.get(resolveURL("/v3/acls"))
      .addHeaders(getHeaders())
      .build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }

    return ObjectResponse.<Set<ACLEntry>>fromJsonBody(response, ACL_SET_TYPE).getResponseObject();
  }

  /**
   * TODO
   *
   * @param query
   * @throws IOException
   */
  public void deleteACLs(ACLStore.Query query) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v3/acls/delete"))
      .withBody(GSON.toJson(query))
      .addHeaders(getHeaders())
      .build();
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
  public void createACL(ACLEntry entry) throws IOException {
    HttpRequest request = HttpRequest.post(resolveURL("/v3/acls"))
      .withBody(GSON.toJson(entry))
      .addHeaders(getHeaders())
      .build();
    HttpResponse response = HttpRequests.execute(request);

    if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Unexpected response: " + response.getResponseCode() +
                              ": " + response.getResponseMessage());
    }
  }

  protected URL resolveURL(String path) throws MalformedURLException {
    return baseURISupplier.get().resolve(path).toURL();
  }

  private Multimap<String, String> getHeaders() {
    return headersSupplier == null ? ImmutableMultimap.<String, String>of() : headersSupplier.get();
  }
}
