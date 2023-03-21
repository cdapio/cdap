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

package io.cdap.cdap.proto.sourcecontrol;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link RepositoryConfig} class.
 */
public class RepositoryConfigTest {
  private static final Provider PROVIDER = Provider.GITHUB;
  private static final String LINK = "example.com";
  private static final String DEFAULT_BRANCH = "develop";
  private static final AuthType AUTH_TYPE = AuthType.PAT;
  private static final String PASSWORD_NAME = "password";
  private static final String USERNAME = "user";
  private static final AuthConfig AUTH_CONFIG = new AuthConfig(
      AUTH_TYPE, new PatConfig(PASSWORD_NAME, USERNAME));

  @Test
  public void testValidRepositoryConfig() {
    RepositoryConfig repo = new RepositoryConfig.Builder().setProvider(PROVIDER)
      .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuth(AUTH_CONFIG).build();

    Assert.assertEquals(PROVIDER, repo.getProvider());
    Assert.assertEquals(LINK, repo.getLink());
    Assert.assertEquals(DEFAULT_BRANCH, repo.getDefaultBranch());
    Assert.assertEquals(AUTH_TYPE, repo.getAuth().getType());
    Assert.assertEquals(PASSWORD_NAME, repo.getAuth().getPatConfig().getPasswordName());
    Assert.assertEquals(USERNAME, repo.getAuth().getPatConfig().getUsername());
  }

  @Test
  public void testInvalidProvider() {
    try {
      new RepositoryConfig.Builder().setProvider(null)
        .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuth(AUTH_CONFIG).build();
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'provider' field must be specified.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidLink() {
    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
        .setLink(null).setDefaultBranch(DEFAULT_BRANCH).setAuth(AUTH_CONFIG).build();
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'link' field must be specified.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidAuthType() {
    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
          .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH)
          .setAuth(new AuthConfig(null, new PatConfig(PASSWORD_NAME, USERNAME)))
          .build();
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'type' must be specified in 'auth'.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidPasswordName() {
    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
          .setLink(LINK)
          .setDefaultBranch(DEFAULT_BRANCH)
          .setAuth(new AuthConfig(AUTH_TYPE, new PatConfig(null, USERNAME)))
          .build();
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'passwordName' must be specified in 'patConfig'.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testMultipleInvalidFields() {
    try {
      new RepositoryConfig.Builder().setProvider(null)
          .setLink(null).setDefaultBranch(DEFAULT_BRANCH)
          .setAuth(new AuthConfig(null, new PatConfig(PASSWORD_NAME, USERNAME)))
          .build();
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertEquals(3, e.getFailures().size());
    }
  }
}
