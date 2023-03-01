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

package io.cdap.cdap.authenticator.gcp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.cdap.proto.security.Credential;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link GCPRemoteAuthenticator}.
 */
public class GCPRemoteAuthenticatorTest {

  @Test
  public void testRemoteAuthenticatorRefreshesNullAccessToken() throws Exception {
    String accessTokenValue = "access-token";
    // This is just an arbitrary fixed point in time.
    Instant fixedInstant = Instant.ofEpochSecond(1646358109);
    Clock fixedClock = Clock.fixed(fixedInstant, ZoneId.systemDefault());
    GoogleCredentials mockGoogleCredentials = mock(GoogleCredentials.class);
    AccessToken accessToken = new AccessToken(accessTokenValue, Date.from(fixedInstant.plus(Duration.ofHours(1))));
    when(mockGoogleCredentials.refreshAccessToken()).thenReturn(accessToken);
    GCPRemoteAuthenticator gcpRemoteAuthenticator = new GCPRemoteAuthenticator(mockGoogleCredentials, fixedClock,
                                                                               null);

    // Verify expected credential value and that refresh was called exactly once.
    Credential credential = gcpRemoteAuthenticator.getCredentials();
    Assert.assertEquals(accessTokenValue, credential.getValue());
    verify(mockGoogleCredentials, times(1)).refreshAccessToken();
  }

  @Test
  public void testRemoteAuthenticatorRefreshesExpiredAccessToken() throws Exception {
    String expiredAccessTokenValue = "expired-access-token";
    String accessTokenValue = "access-token";
    // This is just an arbitrary fixed point in time.
    Instant fixedInstant = Instant.ofEpochSecond(1646358109);
    Clock fixedClock = Clock.fixed(fixedInstant, ZoneId.systemDefault());
    GoogleCredentials mockGoogleCredentials = mock(GoogleCredentials.class);
    AccessToken expiredAccessToken = new AccessToken(expiredAccessTokenValue,
                                                     Date.from(fixedInstant.minus(Duration.ofHours(1))));
    AccessToken accessToken = new AccessToken(accessTokenValue, Date.from(fixedInstant.plus(Duration.ofHours(1))));
    when(mockGoogleCredentials.refreshAccessToken()).thenReturn(accessToken);
    GCPRemoteAuthenticator gcpRemoteAuthenticator = new GCPRemoteAuthenticator(mockGoogleCredentials, fixedClock,
                                                                               expiredAccessToken);

    // Verify expected credential value and that refresh was called exactly once.
    Credential credential = gcpRemoteAuthenticator.getCredentials();
    Assert.assertEquals(accessTokenValue, credential.getValue());
    verify(mockGoogleCredentials, times(1)).refreshAccessToken();
  }

  @Test
  public void testRemoteAuthenticatorReturnsValidAccessToken() throws Exception {
    String accessTokenValue = "access-token";
    // This is just an arbitrary fixed point in time.
    Instant fixedInstant = Instant.ofEpochSecond(1646358109);
    Clock fixedClock = Clock.fixed(fixedInstant, ZoneId.systemDefault());
    GoogleCredentials mockGoogleCredentials = mock(GoogleCredentials.class);
    AccessToken accessToken = new AccessToken(accessTokenValue, Date.from(fixedInstant.plus(Duration.ofHours(1))));
    when(mockGoogleCredentials.refreshAccessToken()).thenReturn(accessToken);
    GCPRemoteAuthenticator gcpRemoteAuthenticator = new GCPRemoteAuthenticator(mockGoogleCredentials, fixedClock,
                                                                               accessToken);

    // Verify expected credential value and that refresh was not called.
    Credential credential = gcpRemoteAuthenticator.getCredentials();
    Assert.assertEquals(accessTokenValue, credential.getValue());
    verify(mockGoogleCredentials, times(0)).refreshAccessToken();
  }
}
