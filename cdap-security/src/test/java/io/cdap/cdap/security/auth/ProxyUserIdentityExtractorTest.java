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

package io.cdap.cdap.security.auth;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Tests related to the {@link ProxyUserIdentityExtractor}.
 */
public class ProxyUserIdentityExtractorTest {
  @Test
  public void testUserIdentityHeaderConfigMissingThrowsException() throws UserIdentityExtractionException {
    String testAuthToken = "test-auth-token";
    CConfiguration config = Mockito.mock(CConfiguration.class);

    ProxyUserIdentityExtractor extractor = new ProxyUserIdentityExtractor(config);

    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.AUTHORIZATION, String.format("Bearer %s", testAuthToken));
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                                        "http://www.example.com", headers);
    UserIdentityExtractionResponse response = extractor.extract(request);
    Assert.assertFalse(response.success());
    Assert.assertEquals(UserIdentityExtractionState.ERROR_MISSING_IDENTITY, response.getState());
  }

  @Test
  public void testEmptyUserThrowsException() throws UserIdentityExtractionException {
    String testUserIdHeader = "X-User-Id";
    String testAuthToken = "test-auth-token";
    CConfiguration config = Mockito.mock(CConfiguration.class);
    when(config.get(Constants.Security.Authentication.AUTH_PROXY_USER_ID_HEADER)).thenReturn(testUserIdHeader);

    ProxyUserIdentityExtractor extractor = new ProxyUserIdentityExtractor(config);

    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.AUTHORIZATION, String.format("Bearer %s", testAuthToken));
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                                        "http://www.example.com", headers);
    UserIdentityExtractionResponse response = extractor.extract(request);
    Assert.assertFalse(response.success());
    Assert.assertEquals(UserIdentityExtractionState.ERROR_MISSING_IDENTITY, response.getState());
  }

  @Test
  public void testValidUserReturnsExpectedIdentity() throws UserIdentityExtractionException {
    String testUserId = "test-user-id";
    String testUserIdHeader = "X-User-Id";
    String testAuthToken = "test-auth-token";
    CConfiguration config = Mockito.mock(CConfiguration.class);
    when(config.get(Constants.Security.Authentication.AUTH_PROXY_USER_ID_HEADER)).thenReturn(testUserIdHeader);

    ProxyUserIdentityExtractor extractor = new ProxyUserIdentityExtractor(config);

    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.AUTHORIZATION, String.format("Bearer %s", testAuthToken));
    headers.add(testUserIdHeader, testUserId);
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                                        "http://www.example.com", headers);
    UserIdentityExtractionResponse response = extractor.extract(request);
    Assert.assertTrue(response.success());
    UserIdentityPair identity = response.getIdentityPair();

    Assert.assertEquals(testAuthToken, identity.getUserCredential());
    Assert.assertEquals(testUserId, identity.getUserIdentity().getUsername());
  }

  @Test
  public void testValidUserWithoutCredentialReturnsExpectedIdentity() throws UserIdentityExtractionException {
    String testUserId = "test-user-id";
    String testUserIdHeader = "X-User-Id";
    CConfiguration config = Mockito.mock(CConfiguration.class);
    when(config.get(Constants.Security.Authentication.AUTH_PROXY_USER_ID_HEADER)).thenReturn(testUserIdHeader);

    ProxyUserIdentityExtractor extractor = new ProxyUserIdentityExtractor(config);

    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.add(testUserIdHeader, testUserId);
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                                                        "http://www.example.com", headers);
    UserIdentityExtractionResponse response = extractor.extract(request);
    Assert.assertTrue(response.success());
    UserIdentityPair identity = response.getIdentityPair();

    Assert.assertEquals(null, identity.getUserCredential());
    Assert.assertEquals(testUserId, identity.getUserIdentity().getUsername());
  }
}
