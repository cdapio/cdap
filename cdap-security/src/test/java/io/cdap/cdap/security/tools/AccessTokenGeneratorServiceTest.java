/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.security.tools;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.security.auth.AccessTokenValidator;
import io.cdap.cdap.security.auth.TokenState;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for access token generator http endpoint
 */
public class AccessTokenGeneratorServiceTest {
  private static AccessTokenGeneratorService tokenGenerator;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    // Don't set port, AccessTokenGeneratorService will find an ephemeral port.
    cConf.setInt(AccessTokenGeneratorService.PORT_CONFIG, 0);
    tokenGenerator  = new AccessTokenGeneratorService();
    tokenGenerator.init(new String[0]);
    tokenGenerator.start();
  }

  @AfterClass
  public static void teardown() throws Exception {
    tokenGenerator.stop();
  }

  @Test
  public void testTokenGenerator() throws Exception {
    String spec = String.format("http://%s:%s/v3Internal/accesstoken?username=root",
                                tokenGenerator.getBindAddress().getAddress().getHostAddress(),
                                tokenGenerator.getBindAddress().getPort());
    URL url = new URL(spec);
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    AccessTokenValidator validator = new AccessTokenValidator(tokenGenerator.getTokenManager(),
                                                              tokenGenerator.getTokenCodec());
    String token = response.getResponseBodyAsString(StandardCharsets.UTF_8);
    TokenState tokenState = validator.validate(token);
    Assert.assertEquals(io.cdap.cdap.security.auth.TokenState.VALID, tokenState);
  }

  @Test
  public void testInvalidIdentifierType() throws Exception {
    String spec = String.format("http://%s:%s/v3Internal/accesstoken?username=root&identifierType=foo",
                                tokenGenerator.getBindAddress().getAddress().getHostAddress(),
                                tokenGenerator.getBindAddress().getPort());
    URL url = new URL(spec);
    HttpRequest request = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse response = HttpRequests.execute(request, new DefaultHttpRequestConfig(false));
    Assert.assertEquals(400, response.getResponseCode());
  }
}
