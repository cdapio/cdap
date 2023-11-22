/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Singleton;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Internal {@link HttpHandler} for credential providers.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3)
public class CredentialProviderHttpHandlerInternal extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(
      BasicThrowable.class, new BasicThrowableCodec()).create();

  private final CredentialProvider credentialProvider;

  @Inject
  CredentialProviderHttpHandlerInternal(CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
  }

  /**
   * Provisions a credential for a given identity.
   *
   * @param request      The HTTP request.
   * @param responder    The HTTP responder.
   * @param namespace    The namespace of the identity for which to provision a credential.
   * @param identityName The name of the identity for which to provision a credential.
   * @throws CredentialProvisioningException If provisioning fails.
   * @throws IOException                     If transport errors occur.
   * @throws NotFoundException               If the identity or associated profile are not found.
   */
  @POST
  @Path("/namespaces/{namespace-id}/credentials/identities/{identity-name}/provision")
  public void provisionCredential(FullHttpRequest request, HttpResponder responder,
      @PathParam("namespace-id") String namespace, @PathParam("identity-name") String identityName)
      throws BadRequestException, CredentialProvisioningException, IOException, NotFoundException {
    CredentialProvisionContext context = deserializeRequestContent(request,
        CredentialProvisionContext.class);
    try {
      responder.sendJson(HttpResponseStatus.OK,
          GSON.toJson(credentialProvider.provision(namespace, identityName, context)));
    } catch (io.cdap.cdap.proto.credential.NotFoundException e) {
      throw new NotFoundException(e.getMessage());
    }
  }

  private <T> T deserializeRequestContent(FullHttpRequest request, Class<T> clazz)
      throws BadRequestException {
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        StandardCharsets.UTF_8)) {
      T content = GSON.fromJson(reader, clazz);
      if (content == null) {
        throw new BadRequestException("No request object provided; expected class "
            + clazz.getName());
      }
      return content;
    } catch (JsonSyntaxException | IOException e) {
      throw new BadRequestException("Unable to parse request: " + e.getMessage(), e);
    }
  }
}
