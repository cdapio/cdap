/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.kerberos.ImpersonationRequest;
import co.cask.cdap.common.kerberos.PrincipalCredentials;
import co.cask.cdap.common.kerberos.UGIWithPrincipal;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.ImpersonationUtils;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.util.concurrent.Callable;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Provides REST endpoint to resolve UGI for a given entity and acquires the delegation tokens for that UGI,
 * using {@link TokenSecureStoreRenewer}, and serializes these Credentials to a location.
 *
 * Response with the location to which the credentials were serialized to, as well as the UGI's short username
 */
// we don't share the same version as other handlers, so we can upgrade/iterate faster
@Path("/v1/impersonation")
public class ImpersonationHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(NamespacedEntityId.class, new EntityIdTypeAdapter())
    .create();

  private final UGIProvider ugiProvider;
  private final TokenSecureStoreRenewer tokenSecureStoreRenewer;
  private final LocationFactory locationFactory;

  @Inject
  ImpersonationHandler(UGIProvider ugiProvider, TokenSecureStoreRenewer tokenSecureStoreRenewer,
                       LocationFactory locationFactory) {
    this.ugiProvider = ugiProvider;
    this.tokenSecureStoreRenewer = tokenSecureStoreRenewer;
    this.locationFactory = locationFactory;
  }

  @POST
  @Path("/credentials")
  public void getCredentials(HttpRequest request, HttpResponder responder) throws Exception {
    String requestContent = request.getContent().toString(Charsets.UTF_8);
    if (requestContent == null) {
      throw new BadRequestException("Request body is empty.");
    }
    ImpersonationRequest impersonationRequest = GSON.fromJson(requestContent, ImpersonationRequest.class);
    LOG.debug("Fetching credentials for {}", impersonationRequest);
    UGIWithPrincipal ugiWithPrincipal = ugiProvider.getConfiguredUGI(impersonationRequest);
    Credentials credentials = ImpersonationUtils.doAs(ugiWithPrincipal.getUGI(), new Callable<Credentials>() {
      @Override
      public Credentials call() throws Exception {
        return tokenSecureStoreRenewer.createCredentials();
      }
    });

    // example: hdfs:///cdap/credentials
    Location credentialsDir = locationFactory.create("credentials");
    if (credentialsDir.isDirectory() || credentialsDir.mkdirs() || credentialsDir.isDirectory()) {

      // the getTempFile() doesn't create the file within the directory that you call it on. It simply appends the path
      // without a separator, which is why we manually append the "tmp"
      // example: hdfs:///cdap/credentials/tmp.5960fe60-6fd8-4f3e-8e92-3fb6d4726006.credentials
      Location credentialsFile = credentialsDir.append("tmp").getTempFile(".credentials");
      // 600 is owner-only READ_WRITE
      try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
        credentialsFile.getOutputStream("600")))) {
        credentials.writeTokenStorageToStream(os);
      }
      LOG.debug("Wrote credentials for user {} to {}", ugiWithPrincipal.getPrincipal(), credentialsFile);
      PrincipalCredentials principalCredentials = new PrincipalCredentials(ugiWithPrincipal.getPrincipal(),
                                                                           credentialsFile.toURI().toString());
      responder.sendJson(HttpResponseStatus.OK, principalCredentials);
    } else {
      throw new IllegalStateException("Unable to create credentials directory.");
    }
  }
}
