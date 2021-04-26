/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * A {@link MacroEvaluator} for resolving the {@code ${oauth(provider, credentialId)}} macro function. It uses
 * the studio service for getting oauth access token at runtime.
 */
public class OAuthMacroEvaluator extends AbstractServiceRetryableMacroEvaluator {

  public static final String FUNCTION_NAME = "oauth";
  private static final String SERVICE_NAME = "Oauth";

  private final ServiceDiscoverer serviceDiscoverer;

  public OAuthMacroEvaluator(ServiceDiscoverer serviceDiscoverer) {
    super(FUNCTION_NAME);
    this.serviceDiscoverer = serviceDiscoverer;
  }

  /**
   * Evaluates the OAuth macro function by calling the OAuth service to exchange an OAuth token.
   *
   * @param args should contains exactly two arguments. The first one is the name of the OAuth provider, and the
   *             second argument is the credential id.
   * @return a OAuth token
   */
  @Override
  public String evaluateMacro(String macroFunction,
                              String... args) throws InvalidMacroException, IOException, RetryableException {
    if (args.length != 2) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 2 arguments");
    }

    return getOAuthToken(args[0], args[1]);
  }

  /**
   * Gets the OAuth token for the given provider and credential ID.
   *
   * @param provider the name of the OAuth provider
   * @param credentialId the ID of the authenticated credential
   * @return an OAuth token
   * @throws IOException if failed to get the OAuth token due to non-retryable error
   * @throws RetryableException if failed to get the OAuth token due to transient error
   */
  private String getOAuthToken(String provider, String credentialId) throws IOException, RetryableException {
    HttpURLConnection urlConn = serviceDiscoverer.openConnection(NamespaceId.SYSTEM.getNamespace(),
                                                                 Constants.PIPELINEID,
                                                                 Constants.STUDIO_SERVICE_NAME,
                                                                 String.format("v1/oauth/provider/%s/credential/%s",
                                                                               provider, credentialId));
    return validateAndRetrieveContent(SERVICE_NAME, urlConn);
  }
}
