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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthClientCredentials;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.AuthType;
import io.cdap.cdap.datapipeline.oauth.RefreshType;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.OAuthStoreException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  private static final String EXPECTED_ORIGIN_ERROR =
      "Cannot change OAuth provider URL scheme, host, or port when reusing client credentials";

  @Before
  public void setUp() {
    mockTransactionRunner = mock(TransactionRunner.class);
    mockSecureStore = mock(SecureStore.class);
    mockSecureStoreManager = mock(SecureStoreManager.class);
    mockTable = mock(StructuredTable.class);
    mockRow = mock(StructuredRow.class);

    oauthStore = new OAuthStore(mockTransactionRunner, mockSecureStore,
        mockSecureStoreManager, Collections.singletonMap("pkce.code.verifier.ttl.sec", "900"));
  }

  @Test
  public void testGetProviderWithNullCredentialStrategy() throws Exception {
    String clientCredsJson = "{\"clientId\":\"test-client\",\"clientSecret\":\"test-secret\"}";
    when(mockSecureStore.getData(any(), any())).thenReturn(
        clientCredsJson.getBytes(StandardCharsets.UTF_8));

    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

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

    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

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

    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

    doNothing().when(mockTable).delete(any());

    oauthStore.deleteProvider(PROVIDER_NAME, false);
    verify(mockSecureStoreManager, times(1)).delete(any(), any());
  }

  @Test
  public void testDeleteProviderPreserveCredentials() throws Exception {
    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

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

    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      runnable.run(mockContext);
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));
    doNothing().when(mockTable).delete(any());

    // CASE 1 :  When Secure keys not found, the provider should be deleted.
    doThrow(new NotFoundException("Keys not found.")).when(mockSecureStoreManager).delete(any(), any());
    oauthStore.deleteProvider(PROVIDER_NAME, false);
    verify(mockSecureStoreManager, times(1)).delete(any(), any());
    verify(mockTable, times(1)).delete(any());

    // CASE 2 :  When secure keys were not deleted because of any reason, the provider should NOT be deleted.
    org.mockito.Mockito.clearInvocations(mockSecureStoreManager);
    org.mockito.Mockito.clearInvocations(mockTable);
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
  public void testWriteProviderStoresOriginMetadataAndRejectsInvalidInitialUrl() throws Exception {
    Map<String, Map<String, String>> rows = new LinkedHashMap<>();
    wireInMemoryTable(rows);

    try {
      oauthStore.writeProvider(
          createProviderWithCreds(PROVIDER_NAME, "https://user@127.0.0.1:8443/login", TOKEN_REFRESH_URL), false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("without userinfo"));
    }
    verify(mockSecureStoreManager, never()).put(anyString(), anyString(), anyString(), anyString(), any());

    String loginUrl = "https://127.0.0.1:8443/login";
    String tokenUrl = "https://127.0.0.1:8443/token";
    oauthStore.writeProvider(createProviderWithCreds(PROVIDER_NAME, loginUrl, tokenUrl), false);

    Map<String, String> expectedProps = ImmutableMap.of("loginurl", loginUrl, "tokenrefreshurl", tokenUrl);
    verify(mockSecureStoreManager, times(1)).put(
        eq("system"), eq("oauthclientcreds-" + PROVIDER_NAME), anyString(), eq("OAuth client creds"),
        eq(expectedProps));
    assertEquals(1, rows.size());
    assertEquals(tokenUrl, rows.get(PROVIDER_NAME).get("tokenrefreshurl"));
  }

  @Test
  public void testWriteProviderReuseFailsBeforeMutatingTableWhenCredentialMissing() throws Exception {
    Map<String, Map<String, String>> rows = new LinkedHashMap<>();
    wireInMemoryTable(rows);
    when(mockSecureStore.getMetadata(eq("system"), eq("oauthclientcreds-" + PROVIDER_NAME))).thenReturn(null);

    try {
      oauthStore.writeProvider(createProviderWithoutCreds(
          PROVIDER_NAME, "https://127.0.0.1:8443/login", "https://127.0.0.1:8443/token"), true);
      fail("Expected OAuthStoreException");
    } catch (OAuthStoreException expected) {
      assertEquals("Client credential should be present in the secure store", expected.getMessage());
    }
    assertTrue(rows.isEmpty());
  }

  @Test
  public void testWriteProviderReuseRejectsOriginChangeFromMetadataBeforeMutatingTable() throws Exception {
    Map<String, Map<String, String>> rows = new LinkedHashMap<>();
    wireInMemoryTable(rows);
    String loginUrl = "https://127.0.0.1:8443/login";
    String tokenUrl = "https://127.0.0.1:8443/token";
    when(mockSecureStore.getMetadata(eq("system"), eq("oauthclientcreds-" + PROVIDER_NAME)))
        .thenReturn(new SecureStoreMetadata(
            "oauthclientcreds-" + PROVIDER_NAME,
            "OAuth client creds",
            0L,
            ImmutableMap.of("loginurl", loginUrl, "tokenrefreshurl", tokenUrl)));

    assertReuseOriginRejected(PROVIDER_NAME, loginUrl, "https://127.0.0.2:8443/steal");
    assertReuseOriginRejected(PROVIDER_NAME, "http://127.0.0.1:8443/login", tokenUrl);
    assertReuseOriginRejected(PROVIDER_NAME, loginUrl, "https://127.0.0.1:9443/token");
    assertReuseOriginRejected(PROVIDER_NAME, loginUrl, "https://user@127.0.0.1:8443/token");

    // Partial metadata (single URL property) must still enforce origin on both URLs.
    when(mockSecureStore.getMetadata(eq("system"), eq("oauthclientcreds-" + PROVIDER_NAME)))
        .thenReturn(new SecureStoreMetadata(
            "oauthclientcreds-" + PROVIDER_NAME, "OAuth client creds", 0L, ImmutableMap.of("loginurl", loginUrl)));
    assertReuseOriginRejected(PROVIDER_NAME, loginUrl, tokenUrl);
    assertTrue(rows.isEmpty());
  }

  @Test
  public void testWriteProviderReuseEnforcesMetadataAcrossPreserveAndCaseVariants() throws Exception {
    Map<String, Map<String, String>> rows = new LinkedHashMap<>();
    Map<String, SecureStoreMetadata> secrets = new LinkedHashMap<>();
    wireInMemoryTable(rows);
    wireInMemorySecureStore(secrets);

    String loginUrl = "https://127.0.0.1:8443/login";
    String tokenUrl = "https://127.0.0.1:8443/token";
    oauthStore.writeProvider(createProviderWithCreds("corp", loginUrl, tokenUrl), false);

    // Case-variant provider name resolves to the same lowercased Secure Store key and is blocked.
    assertReuseOriginRejected("CORP", loginUrl, "https://127.0.0.2:8443/steal");
    assertFalse(rows.containsKey("CORP"));

    // Deleting with preserve_client_credentials=true removes the table row while preserving metadata.
    oauthStore.deleteProvider("corp", true);
    assertFalse(rows.containsKey("corp"));
    assertTrue(secrets.containsKey("oauthclientcreds-corp"));
    assertReuseOriginRejected("corp", loginUrl, "https://127.0.0.2:8443/steal");

    // Same-origin re-creation after preserve succeeds.
    oauthStore.writeProvider(
        createProviderWithoutCreds("corp", "https://127.0.0.1:8443/login2", "https://127.0.0.1:8443/token2"), true);
    assertEquals("https://127.0.0.1:8443/token2", rows.get("corp").get("tokenrefreshurl"));

    // Full delete removes both table row and Secure Store entry.
    oauthStore.deleteProvider("corp", false);
    assertFalse(rows.containsKey("corp"));
    assertFalse(secrets.containsKey("oauthclientcreds-corp"));
  }

  @Test
  public void testWriteProviderReuseWithEmptyMetadataEnforcesCaseInsensitiveRowOrigin() throws Exception {
    Map<String, Map<String, String>> rows = new LinkedHashMap<>();
    wireInMemoryTable(rows);
    when(mockSecureStore.getMetadata(eq("system"), eq("oauthclientcreds-legacy")))
        .thenReturn(new SecureStoreMetadata("oauthclientcreds-legacy", "Legacy", 0L, Collections.emptyMap()));

    // First bind with empty metadata rejects invalid URLs, then allows valid initial bind.
    try {
      oauthStore.writeProvider(createProviderWithoutCreds(
          "legacy", "https://user@127.0.0.1:8443/login", "https://127.0.0.1:8443/token"), true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("without userinfo"));
    }

    oauthStore.writeProvider(createProviderWithoutCreds(
        "legacy", "https://127.0.0.1:8443/login", "https://127.0.0.1:8443/token"), true);
    assertEquals("https://127.0.0.1:8443/token", rows.get("legacy").get("tokenrefreshurl"));

    // Exact-case and case-variant ("LEGACY") origin swaps are blocked by the active row scan.
    assertReuseOriginRejected("legacy", "https://127.0.0.1:8443/login", "https://127.0.0.2:8443/steal");
    assertReuseOriginRejected("legacy", "http://127.0.0.1:8443/login", "https://127.0.0.1:8443/token");
    assertReuseOriginRejected("LEGACY", "https://127.0.0.1:8443/login", "https://127.0.0.2:8443/steal");
    assertFalse(rows.containsKey("LEGACY"));

    // Same-origin update succeeds.
    oauthStore.writeProvider(createProviderWithoutCreds(
        "legacy", "https://127.0.0.1:8443/login2", "https://127.0.0.1:8443/token2"), true);
    assertEquals("https://127.0.0.1:8443/token2", rows.get("legacy").get("tokenrefreshurl"));
  }

  @Test
  public void testWriteProviderWrapsTableIOException() throws Exception {
    wireInMemoryTable(new LinkedHashMap<>());
    IOException ioFailure = new IOException("disk full");
    doThrow(ioFailure).when(mockTable).upsert(any());

    try {
      oauthStore.writeProvider(createProviderWithCreds(PROVIDER_NAME, LOGIN_URL, TOKEN_REFRESH_URL), false);
      fail("Expected OAuthStoreException");
    } catch (OAuthStoreException expected) {
      assertEquals("Failed to write OAuth provider to table", expected.getMessage());
      assertSame(ioFailure, expected.getCause());
    }
  }

  private void assertReuseOriginRejected(String name, String loginUrl, String tokenUrl) throws Exception {
    try {
      oauthStore.writeProvider(createProviderWithoutCreds(name, loginUrl, tokenUrl), true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertEquals(EXPECTED_ORIGIN_ERROR, expected.getMessage());
    }
  }

  private static OAuthProvider createProviderWithCreds(String name, String loginUrl, String tokenUrl) {
    OAuthClientCredentials creds =
        OAuthClientCredentials.newBuilder().withClientId("cid").withClientSecret("csecret").build();
    return providerBuilder(name, loginUrl, tokenUrl).withClientCredentials(creds).build();
  }

  private static OAuthProvider createProviderWithoutCreds(String name, String loginUrl, String tokenUrl) {
    return providerBuilder(name, loginUrl, tokenUrl).build();
  }

  private static OAuthProvider.Builder providerBuilder(String name, String loginUrl, String tokenUrl) {
    return OAuthProvider.newBuilder()
        .withName(name)
        .withLoginURL(loginUrl)
        .withTokenRefreshURL(tokenUrl)
        .withUserAgent(USER_AGENT);
  }

  private void wireInMemorySecureStore(Map<String, SecureStoreMetadata> secrets) throws Exception {
    doAnswer(invocation -> {
      String key = invocation.getArgument(1);
      String description = invocation.getArgument(3);
      @SuppressWarnings("unchecked")
      Map<String, String> properties = invocation.getArgument(4);
      secrets.put(key, new SecureStoreMetadata(key, description, 0L, properties));
      return null;
    }).when(mockSecureStoreManager).put(eq("system"), anyString(), anyString(), anyString(), any());

    when(mockSecureStore.getMetadata(eq("system"), anyString()))
        .thenAnswer(invocation -> secrets.get(invocation.getArgument(1)));

    doAnswer(invocation -> {
      secrets.remove(invocation.getArgument(1));
      return null;
    }).when(mockSecureStoreManager).delete(eq("system"), anyString());
  }

  private void wireInMemoryTable(Map<String, Map<String, String>> rows) throws Exception {
    doAnswer(invocation -> {
      TxRunnable runnable = invocation.getArgument(0);
      StructuredTableContext mockContext = mock(StructuredTableContext.class);
      when(mockContext.getTable(any())).thenReturn(mockTable);
      try {
        runnable.run(mockContext);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new TransactionException("Transaction failed", e);
      }
      return null;
    }).when(mockTransactionRunner).run(any(TxRunnable.class));

    doAnswer(invocation -> {
      Collection<Field<?>> fields = invocation.getArgument(0);
      Map<String, String> values = new LinkedHashMap<>();
      for (Field<?> field : fields) {
        Object value = field.getValue();
        if (value != null) {
          values.put(field.getName(), value.toString());
        }
      }
      rows.put(values.get("oauthprovider"), values);
      return null;
    }).when(mockTable).upsert(any());

    when(mockTable.read(any())).thenAnswer(invocation -> {
      Collection<Field<?>> keys = invocation.getArgument(0);
      String key = keys.iterator().next().getValue().toString();
      Map<String, String> values = rows.get(key);
      return values == null ? Optional.empty() : Optional.of(toStructuredRow(values));
    });

    when(mockTable.scan(any(), anyInt())).thenAnswer(invocation -> {
      List<StructuredRow> snapshot = new ArrayList<>();
      for (Map<String, String> values : rows.values()) {
        snapshot.add(toStructuredRow(values));
      }
      Iterator<StructuredRow> iterator = snapshot.iterator();
      return new CloseableIterator<StructuredRow>() {
        @Override
        public void close() {
          // no-op
        }

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public StructuredRow next() {
          return iterator.next();
        }
      };
    });

    doAnswer(invocation -> {
      Collection<Field<?>> keys = invocation.getArgument(0);
      String key = keys.iterator().next().getValue().toString();
      rows.remove(key);
      return null;
    }).when(mockTable).delete(any());
  }

  private static StructuredRow toStructuredRow(Map<String, String> values) {
    StructuredRow row = mock(StructuredRow.class);
    when(row.getString(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
    return row;
  }
}
