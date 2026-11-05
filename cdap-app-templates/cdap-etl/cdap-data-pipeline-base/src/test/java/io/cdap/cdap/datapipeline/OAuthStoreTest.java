/*
 * Copyright © 2025 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.datapipeline.oauth.AuthType;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthClientCredentials;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.OAuthStoreException;
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class OAuthStoreTest {
  private OAuthStore oauthStore;
  private TransactionRunner mockTransactionRunner;
  private SecureStore mockSecureStore;
  private SecureStoreManager mockSecureStoreManager;
  private StructuredTable mockTable;
  private StructuredRow mockRow;

  private static final String PROVIDER_NAME = "test-provider";
  private static final String TOKEN_REFRESH_URL = "http://token.example.com";
  private static final String LOGIN_URL = "http://login.example.com";
  private static final String USER_AGENT = "test-Agent";

  @Before
  public void setUp() throws Exception {
    mockTransactionRunner = mock(TransactionRunner.class);
    mockSecureStore = mock(SecureStore.class);
    mockSecureStoreManager = mock(SecureStoreManager.class);
    mockTable = mock(StructuredTable.class);
    mockRow = mock(StructuredRow.class);

    when(mockTable.read(any())).thenReturn(Optional.empty());
    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

    oauthStore = new OAuthStore(mockTransactionRunner, mockSecureStore,
        mockSecureStoreManager, Collections.singletonMap("pkce.code.verifier.ttl.sec", "900"));
  }

  @Test
  public void testGetProviderWithNullCredentialStrategy() throws Exception {
    String clientCredsJson = "{\"clientId\":\"test-client\",\"clientSecret\":\"test-secret\"}";
    when(mockSecureStore.getData(any(), any())).thenReturn(
        clientCredsJson.getBytes(StandardCharsets.UTF_8));

    when(mockRow.getString("oauthprovider")).thenReturn(PROVIDER_NAME);
    when(mockRow.getString("loginurl")).thenReturn(LOGIN_URL);
    when(mockRow.getString("tokenrefreshurl")).thenReturn(TOKEN_REFRESH_URL);
    when(mockRow.getString("credentialencodingstrategy")).thenReturn(null);
    when(mockRow.getString("useragent")).thenReturn(USER_AGENT);

    when(mockTable.read(any())).thenReturn(Optional.of(mockRow));

    Optional<OAuthProvider> provider = oauthStore.getProvider(PROVIDER_NAME);

    assertTrue(provider.isPresent());
    assertEquals(provider.get().getCredentialEncodingStrategy(),
        OAuthProvider.CredentialEncodingStrategy.FORM_BODY);
    assertEquals(provider.get().getAuthType(), AuthType.STANDARD);
    assertEquals(provider.get().getRefreshType(), RefreshType.STANDARD);
  }

  @Test
  public void testGetProviderWithRefreshTokenRotationOAuthType() throws Exception {
    String clientCredsJson = "{\"clientId\":\"test-client\",\"clientSecret\":\"test-secret\"}";
    when(mockSecureStore.getData(any(), any())).thenReturn(
        clientCredsJson.getBytes(StandardCharsets.UTF_8));

    when(mockRow.getString("oauthprovider")).thenReturn(PROVIDER_NAME);
    when(mockRow.getString("loginurl")).thenReturn(LOGIN_URL);
    when(mockRow.getString("tokenrefreshurl")).thenReturn(TOKEN_REFRESH_URL);
    when(mockRow.getString("credentialencodingstrategy")).thenReturn("FORM_BODY");
    when(mockRow.getString("useragent")).thenReturn(USER_AGENT);
    when(mockRow.getString("refreshtype")).thenReturn("RTR");
    when(mockRow.getString("authtype")).thenReturn("PKCE");

    when(mockTable.read(any())).thenReturn(Optional.of(mockRow));

    Optional<OAuthProvider> provider = oauthStore.getProvider(PROVIDER_NAME);

    assertTrue(provider.isPresent());
    assertEquals(provider.get().getAuthType(), AuthType.PKCE);
    assertEquals(provider.get().getRefreshType(), RefreshType.RTR);
  }

  @Test
  public void testWriteRefreshToken() throws Exception {
    doNothing().when(mockSecureStoreManager).put(anyString(), anyString(), any(), anyString(), any());

    OAuthRefreshToken token = OAuthRefreshToken.newBuilder()
        .withRefreshToken("muhtoken")
        .withRedirectURI("uri")
        .build();
    oauthStore.writeRefreshToken("Provider", "ID0", token);

    verify(mockSecureStoreManager, times(1))
        .put(eq("system"), eq("oauthrefreshtoken-provider-id0"), any(), eq("OAuth refresh token"), any());
  }

  @Test
  public void testWriteAccessToken() throws Exception {
    doNothing().when(mockSecureStoreManager).put(anyString(), anyString(), any(), anyString(), any());

    OAuthAccessToken token = OAuthAccessToken.newBuilder()
        .withAccessToken("muhtoken")
        .build();
    oauthStore.writeAccessToken("Provider", "ID0", token);

    verify(mockSecureStoreManager, times(1))
        .put(eq("system"), eq("oauthaccesstoken-provider-id0"), any(), eq("OAuth access token"), any());
  }

  @Test
  public void testDeleteProvider() throws Exception {
    doNothing().when(mockSecureStoreManager).delete(any(), any());
    doNothing().when(mockTable).delete(any());

    oauthStore.deleteProvider(PROVIDER_NAME, false);
    verify(mockSecureStoreManager, times(1)).delete(any(), any());
  }

  @Test
  public void testDeleteProviderPreserveCredentials() throws Exception {
    doNothing().when(mockTable).delete(any());

    oauthStore.deleteProvider(PROVIDER_NAME, true);
    verify(mockSecureStoreManager, times(0)).delete(any(), any());
    verify(mockTable, times(1)).delete(any());
  }

  @Test
  public void testDeleteProviderWhenSecureKeyNotFound() throws Exception {
    class NotFoundException extends Exception {
      public NotFoundException(String message) {
        super(message);
      }
    }

    doNothing().when(mockTable).delete(any());

    // CASE 1 :  When Secure keys not found, the provider should be deleted.
    doThrow(new NotFoundException("Keys not found.")).when(mockSecureStoreManager).delete(any(), any());
    oauthStore.deleteProvider(PROVIDER_NAME, false);
    verify(mockSecureStoreManager, times(1)).delete(any(), any());
    verify(mockTable, times(1)).delete(any());

    // CASE 2 :  When secure keys were not deleted because of any reason, the provider should NOT be deleted.
    clearInvocations(mockSecureStoreManager);
    clearInvocations(mockTable);
    doThrow(new Exception("Unable to delete secure key")).when(mockSecureStoreManager).delete(any(), any());
    try {
      oauthStore.deleteProvider(PROVIDER_NAME, false);
    } catch (Exception e) {
      assertEquals(e.getClass(), OAuthStoreException.class);
    }
    verify(mockSecureStoreManager, times(1)).delete(any(), any());
    verify(mockTable, times(0)).delete(any());
  }

  @Test
  public void testAcquireAndReleaseSecretLease() throws Exception {
    when(mockSecureStoreManager.acquireLease(anyString(), anyString(), anyLong(), anyString()))
        .thenReturn(true);

    assertTrue(oauthStore.acquireLease(PROVIDER_NAME, "cred-1", 30000L, "worker-1"));

    oauthStore.releaseLease(PROVIDER_NAME, "cred-1", "worker-1");
    verify(mockSecureStoreManager, times(1)).releaseLease(anyString(), anyString(), eq("worker-1"));
  }

  @Test
  public void testWriteProviderReuseCredentialsMissingSecretDoesNotUpsertTable() throws Exception {
    OAuthProvider provider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();

    // Case 1: getMetadata returns null
    when(mockSecureStore.getMetadata(anyString(), anyString())).thenReturn(null);
    try {
      oauthStore.writeProvider(provider, true);
      fail("Expected OAuthStoreException when client credentials are missing in SecureStore");
    } catch (OAuthStoreException expected) {
      assertEquals("Client credential should be present in the secure store", expected.getMessage());
      assertNull(expected.getCause());
    }
    verify(mockTable, times(0)).upsert(any());

    // Case 2: getMetadata throws IOException
    IOException ioError = new IOException("store error");
    doThrow(ioError).when(mockSecureStore).getMetadata(anyString(), anyString());
    try {
      oauthStore.writeProvider(provider, true);
      fail("Expected OAuthStoreException when getMetadata throws IOException");
    } catch (OAuthStoreException expected) {
      assertEquals("Failed to check existence of client credential in secure storage", expected.getMessage());
      assertSame(ioError, expected.getCause());
    }
    verify(mockTable, times(0)).upsert(any());

    // Case 3: getMetadata throws non-IOException (e.g. NotFoundException)
    Exception notFoundError = new Exception("not found");
    doThrow(notFoundError).when(mockSecureStore).getMetadata(anyString(), anyString());
    try {
      oauthStore.writeProvider(provider, true);
      fail("Expected OAuthStoreException when getMetadata throws non-IOException");
    } catch (OAuthStoreException expected) {
      assertEquals("Namespace \"system\" or name \"oauthclientcreds-test-provider\" does not exist",
          expected.getMessage());
      assertSame(notFoundError, expected.getCause());
    }
    verify(mockTable, times(0)).upsert(any());
  }

  @Test
  public void testWriteProviderReuseCredentialsRejectsHostOrSchemeChange() throws Exception {
    String expectedErrorMsg =
        "Cannot change OAuth provider URL scheme, host, or port when reusing client credentials";

    assertTrue(OAuthStore.hasSameOrigin(
        "http://login.example.com/v1/auth", "http://login.example.com:80/v2/auth"));
    assertFalse(OAuthStore.hasSameOrigin(
        "https://user:pass@login.example.com/v1/auth", "https://login.example.com/v1/auth"));
    assertFalse(OAuthStore.hasSameOrigin(
        "https://login.example.com/v1/auth", "https://user:pass@login.example.com/v1/auth"));

    when(mockSecureStore.getMetadata(anyString(), anyString()))
        .thenReturn(new SecureStoreMetadata("oauthclientcreds", "desc", 0L, Collections.emptyMap()));

    // Initial creation when no existing row is present in StructuredTable succeeds
    when(mockTable.read(any())).thenReturn(Optional.empty());
    OAuthProvider initialProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    oauthStore.writeProvider(initialProvider, true);
    verify(mockTable, times(1)).upsert(any());
    verify(mockSecureStoreManager, times(0)).put(any(), any(), any(), any(), any());

    clearInvocations(mockTable);

    when(mockRow.getString("loginurl")).thenReturn("https://login.example.com/v1/auth");
    when(mockRow.getString("tokenrefreshurl")).thenReturn("https://token.example.com/v1/token");
    when(mockTable.read(any())).thenReturn(Optional.of(mockRow));

    // Same host, scheme, and effective default port (case-insensitive) with updated path succeeds
    OAuthProvider sameHostProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("HTTPS://LOGIN.EXAMPLE.COM:443/v2/auth")
        .withTokenRefreshURL("https://token.example.com/v2/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    oauthStore.writeProvider(sameHostProvider, true);
    verify(mockTable, times(1)).upsert(any());
    verify(mockSecureStoreManager, times(0)).put(any(), any(), any(), any(), any());

    clearInvocations(mockTable);

    // Changing tokenRefreshURL host is rejected
    OAuthProvider changedHostProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://attacker.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    try {
      oauthStore.writeProvider(changedHostProvider, true);
      fail("Expected IllegalArgumentException when changing tokenRefreshURL host");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Changing loginURL host is rejected
    OAuthProvider changedLoginHostProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://attacker.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    try {
      oauthStore.writeProvider(changedLoginHostProvider, true);
      fail("Expected IllegalArgumentException when changing loginURL host");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Changing scheme is rejected
    OAuthProvider changedSchemeProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("http://token.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    try {
      oauthStore.writeProvider(changedSchemeProvider, true);
      fail("Expected IllegalArgumentException when changing tokenRefreshURL scheme");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Changing port is rejected
    OAuthProvider changedPortProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com:8443/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    try {
      oauthStore.writeProvider(changedPortProvider, true);
      fail("Expected IllegalArgumentException when changing tokenRefreshURL port");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Adding userinfo to URL is rejected
    OAuthProvider userInfoProvider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://user:pass@login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    try {
      oauthStore.writeProvider(userInfoProvider, true);
      fail("Expected IllegalArgumentException when loginURL contains userinfo");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Two-step writeProvider(provider, false) followed by deleteProvider(preserve=true) and
    // writeProvider(reuse=true) when table row is absent still rejects host/scheme/port changes
    OAuthProvider providerWithCreds = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withClientCredentials(OAuthClientCredentials.newBuilder()
            .withClientId("id")
            .withClientSecret("secret")
            .build())
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();
    oauthStore.writeProvider(providerWithCreds, false);
    oauthStore.deleteProvider(PROVIDER_NAME, true);
    when(mockTable.read(any())).thenReturn(Optional.empty());
    clearInvocations(mockTable);

    try {
      oauthStore.writeProvider(changedHostProvider, true);
      fail("Expected IllegalArgumentException after deleteProvider(preserve=true) when changing host");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Fresh OAuthStore instance (simulating restart) where only deleteProvider(preserve=true) reads the row
    // before table deletion still rejects host change via preservedProviderUrls
    OAuthStore freshStoreAfterDelete = new OAuthStore(mockTransactionRunner, mockSecureStore,
        mockSecureStoreManager, Collections.singletonMap("pkce.code.verifier.ttl.sec", "900"));
    when(mockTable.read(any())).thenReturn(Optional.of(mockRow));
    freshStoreAfterDelete.deleteProvider(PROVIDER_NAME, true);
    when(mockTable.read(any())).thenReturn(Optional.empty());
    clearInvocations(mockTable);

    try {
      freshStoreAfterDelete.writeProvider(changedHostProvider, true);
      fail("Expected IllegalArgumentException when preservedProviderUrls from deleteProvider rejects host change");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());

    // Fresh OAuthStore instance where only SecureStoreMetadata.getProperties() contains the original URLs
    // still rejects host change even when table row and preservedProviderUrls are both empty
    OAuthStore freshStoreWithMetadataProps = new OAuthStore(mockTransactionRunner, mockSecureStore,
        mockSecureStoreManager, Collections.singletonMap("pkce.code.verifier.ttl.sec", "900"));
    java.util.Map<String, String> storedProps = new java.util.HashMap<>();
    storedProps.put("loginurl", "https://login.example.com/v1/auth");
    storedProps.put("tokenrefreshurl", "https://token.example.com/v1/token");
    when(mockSecureStore.getMetadata(anyString(), anyString()))
        .thenReturn(new SecureStoreMetadata("oauthclientcreds", "desc", 0L, storedProps));

    try {
      freshStoreWithMetadataProps.writeProvider(changedHostProvider, true);
      fail("Expected IllegalArgumentException when SecureStoreMetadata properties reject host change");
    } catch (IllegalArgumentException expected) {
      assertEquals(expectedErrorMsg, expected.getMessage());
    }
    verify(mockTable, times(0)).upsert(any());
  }

  @Test
  public void testWriteProviderSecureStoreFailureDoesNotUpsertTable() throws Exception {
    doThrow(new IOException("write failed"))
        .when(mockSecureStoreManager).put(anyString(), anyString(), anyString(), anyString(), any());

    OAuthProvider provider = OAuthProvider.newBuilder()
        .withName(PROVIDER_NAME)
        .withLoginURL("https://login.example.com/v1/auth")
        .withTokenRefreshURL("https://token.example.com/v1/token")
        .withClientCredentials(OAuthClientCredentials.newBuilder()
            .withClientId("id")
            .withClientSecret("secret")
            .build())
        .withCredentialEncodingStrategy(OAuthProvider.CredentialEncodingStrategy.FORM_BODY)
        .build();

    try {
      oauthStore.writeProvider(provider, false);
      fail("Expected OAuthStoreException when SecureStoreManager.put fails");
    } catch (OAuthStoreException expected) {
      // expected
    }
    verify(mockTable, times(0)).upsert(any());
  }
}
