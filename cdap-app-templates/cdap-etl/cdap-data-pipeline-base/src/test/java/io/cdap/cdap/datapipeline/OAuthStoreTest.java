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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.datapipeline.oauth.OAuthAccessToken;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthRefreshToken;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.OAuthStoreException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
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
}
