/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.transport.TransportHttp;
import org.junit.Before;
import org.junit.Test;

public class HttpAccessTokenAuthenticationStrategyTest {

  private static final String NAMESPACE = "test-ns";
  private static final String TOKEN_KEY = "token-key";
  private static final String TOKEN_VALUE = "mock-token";

  private SecureStore mockSecureStore;
  private RepositoryConfig mockRepoConfig;

  @Before
  public void setup() throws Exception {
    mockSecureStore = mock(SecureStore.class);
    when(mockSecureStore.getData(NAMESPACE, TOKEN_KEY))
        .thenReturn(TOKEN_VALUE.getBytes(StandardCharsets.UTF_8));

    PatConfig patConfig = new PatConfig(TOKEN_KEY, null);
    AuthConfig authConfig = new AuthConfig(AuthType.HTTP_ACCESS_TOKEN, patConfig);

    mockRepoConfig = new RepositoryConfig.Builder()
        .setProvider(Provider.BITBUCKET_SERVER)
        .setLink("http://fake.repo.com")
        .setDefaultBranch("main")
        .setAuth(authConfig)
        .build();
  }

  @Test
  public void testConfigureSetsBearerHeader() throws Exception {
    HttpAccessTokenAuthenticationStrategy strategy =
        new HttpAccessTokenAuthenticationStrategy(mockSecureStore, mockRepoConfig, NAMESPACE);

    RefreshableCredentialsProvider provider = strategy.getCredentialsProvider();
    provider.refresh();

    HttpAccessTokenAuthenticationStrategy.HttpAccessTokenCredentialsProvider tokenProvider =
        (HttpAccessTokenAuthenticationStrategy.HttpAccessTokenCredentialsProvider) provider;

    TransportCommand<?, ?> mockCommand = mock(TransportCommand.class);
    TransportHttp mockTransport = mock(TransportHttp.class);

    doAnswer(invocation -> {
      Object callback = invocation.getArgument(0);
      ((org.eclipse.jgit.api.TransportConfigCallback) callback).configure(mockTransport);
      return null;
    }).when(mockCommand).setTransportConfigCallback(any());

    tokenProvider.configure(mockCommand);

    verify(mockTransport).setAdditionalHeaders(
        Collections.singletonMap("Authorization", "Bearer " + TOKEN_VALUE));
  }
}
