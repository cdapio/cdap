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

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.datapipeline.oauth.OAuthProvider;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;

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

    oauthStore = new OAuthStore(mockTransactionRunner, mockSecureStore, mockSecureStoreManager);
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
}
