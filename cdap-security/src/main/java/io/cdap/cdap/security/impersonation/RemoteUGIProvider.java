/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.impersonation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

/**
 * Makes requests to ImpersonationHandler to request credentials.
 */
public class RemoteUGIProvider extends AbstractCachedUGIProvider {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteUGIProvider.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new EntityIdTypeAdapter())
    .create();

  private final RemoteClient remoteClient;
  private final LocationFactory locationFactory;

  @Inject
  RemoteUGIProvider(CConfiguration cConf,
                    LocationFactory locationFactory, OwnerAdmin ownerAdmin, RemoteClientFactory remoteClientFactory) {
    super(cConf, ownerAdmin);
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP, new DefaultHttpRequestConfig(false), "/v1/");
    this.locationFactory = locationFactory;
  }

  @Override
  protected UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest)
    throws AccessException {
    ImpersonationRequest jsonRequest = new ImpersonationRequest(impersonationRequest.getEntityId(),
                                                                impersonationRequest.getImpersonatedOpType(),
                                                                impersonationRequest.getPrincipal());
    PrincipalCredentials principalCredentials =
      null;
    try {
      principalCredentials = GSON.fromJson(executeRequest(jsonRequest).getResponseBodyAsString(),
                                           PrincipalCredentials.class);
      LOG.debug("Received response: {}", principalCredentials);
    } catch (IOException e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }

    Location location = locationFactory.create(URI.create(principalCredentials.getCredentialsPath()));
    try {
      String user = principalCredentials.getPrincipal();
      if (impersonationRequest.getImpersonatedOpType() == ImpersonatedOpType.EXPLORE) {
        // For explore operations, we use the short name in UserGroupInformation, to avoid an incorrect
        // check in Hive. See CDAP-12930
        user = new KerberosName(user).getShortName();
      }
      UserGroupInformation impersonatedUGI = UserGroupInformation.createRemoteUser(user);
      impersonatedUGI.addCredentials(readCredentials(location));
      return new UGIWithPrincipal(principalCredentials.getPrincipal(), impersonatedUGI);
    } catch (IOException e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    } finally {
      try {
        if (!location.delete()) {
          LOG.warn("Failed to delete location: {}", location);
        }
      } catch (IOException e) {
        LOG.warn("Exception raised when deleting location {}", location, e);
      }
    }
  }

  /**
   * In remote mode, we should not cache the explore request
   */
  @Override
  protected boolean checkExploreAndDetermineCache(ImpersonationRequest impersonationRequest) {
    return !(impersonationRequest.getEntityId().getEntityType().equals(EntityType.NAMESPACE) &&
      impersonationRequest.getImpersonatedOpType().equals(ImpersonatedOpType.EXPLORE));
  }

  private HttpResponse executeRequest(ImpersonationRequest impersonationRequest)
    throws IOException, UnauthorizedException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "impersonation/credentials")
      .withBody(GSON.toJson(impersonationRequest))
      .build();
    HttpResponse response = remoteClient.execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return response;
    }
    throw new IOException(String.format("%s Response: %s.", createErrorMessage(request.getURL()), response));
  }

  // creates error message, encoding details about the request
  private static String createErrorMessage(URL url) {
    return String.format("Error making request to AppFabric Service at %s.", url);
  }

  private static Credentials readCredentials(Location location) throws IOException {
    Credentials credentials = new Credentials();
    try (DataInputStream input = new DataInputStream(new BufferedInputStream(location.getInputStream()))) {
      credentials.readTokenStorageStream(input);
    }
    LOG.debug("Read credentials from {}", location);
    return credentials;
  }
}
