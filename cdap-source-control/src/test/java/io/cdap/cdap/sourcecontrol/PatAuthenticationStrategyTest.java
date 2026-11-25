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

package io.cdap.cdap.sourcecontrol;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.eclipse.jgit.transport.CredentialItem;
import org.eclipse.jgit.transport.URIish;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link PatAuthenticationStrategy}.
 */
public class PatAuthenticationStrategyTest {

  private static final String NAMESPACE = "ns1";
  private static final String KEY_NAME = "git-pat-key";
  private static final String USERNAME = "git-user";

  @Test
  public void testRefreshSuccessPopulatesCredentials() throws Exception {
    SecureStore secureStore = Mockito.mock(SecureStore.class);
    Mockito.when(secureStore.getData(NAMESPACE, KEY_NAME))
        .thenReturn("my-secret-pat".getBytes(StandardCharsets.UTF_8));

    PatAuthenticationStrategy strategy =
        new PatAuthenticationStrategy(secureStore, createRepoConfig(), NAMESPACE);
    RefreshableCredentialsProvider provider = strategy.getCredentialsProvider();

    provider.refresh();
    CredentialItem.Username userItem = new CredentialItem.Username();
    CredentialItem.Password passItem = new CredentialItem.Password();
    Assert.assertTrue(provider.get(new URIish("https://github.com/org/repo.git"),
        userItem, passItem));
    Assert.assertEquals(USERNAME, userItem.getValue());
    Assert.assertArrayEquals("my-secret-pat".toCharArray(), passItem.getValue());
  }

  @Test
  public void testRefreshPropagatesIOExceptionDirectly() throws Exception {
    SecureStore secureStore = Mockito.mock(SecureStore.class);
    IOException ioError = new IOException("secure store IO failure");
    Mockito.when(secureStore.getData(NAMESPACE, KEY_NAME)).thenThrow(ioError);

    PatAuthenticationStrategy strategy =
        new PatAuthenticationStrategy(secureStore, createRepoConfig(), NAMESPACE);
    try {
      strategy.getCredentialsProvider().refresh();
      Assert.fail("Expected IOException directly");
    } catch (IOException actual) {
      Assert.assertSame(ioError, actual);
    }
  }

  @Test
  public void testRefreshWrapsNonIOExceptionInAuthenticationConfigException() throws Exception {
    SecureStore secureStore = Mockito.mock(SecureStore.class);
    Exception nonIoError = new Exception("key lookup failed");
    Mockito.when(secureStore.getData(NAMESPACE, KEY_NAME)).thenThrow(nonIoError);

    PatAuthenticationStrategy strategy =
        new PatAuthenticationStrategy(secureStore, createRepoConfig(), NAMESPACE);
    try {
      strategy.getCredentialsProvider().refresh();
      Assert.fail("Expected AuthenticationConfigException");
    } catch (AuthenticationConfigException actual) {
      Assert.assertSame(nonIoError, actual.getCause());
    }
  }

  @Test
  public void testRefreshNullDataThrowsAuthenticationConfigException() throws Exception {
    SecureStore secureStore = Mockito.mock(SecureStore.class);
    Mockito.when(secureStore.getData(NAMESPACE, KEY_NAME)).thenReturn(null);

    PatAuthenticationStrategy strategy =
        new PatAuthenticationStrategy(secureStore, createRepoConfig(), NAMESPACE);
    try {
      strategy.getCredentialsProvider().refresh();
      Assert.fail("Expected AuthenticationConfigException when token data is null");
    } catch (AuthenticationConfigException expected) {
      Assert.assertTrue(expected.getMessage().contains(KEY_NAME));
    }
  }

  private static RepositoryConfig createRepoConfig() {
    return new RepositoryConfig.Builder()
        .setProvider(Provider.GITHUB)
        .setLink("https://github.com/org/repo.git")
        .setDefaultBranch("main")
        .setAuth(new AuthConfig(AuthType.PAT, new PatConfig(KEY_NAME, USERNAME)))
        .build();
  }
}
