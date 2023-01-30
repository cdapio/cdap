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
 * Tests for RepositoryConfig class
 */
public class RepositoryConfigTest {
  private static final Provider PROVIDER = Provider.GITHUB;
  private static final String LINK = "example.com";
  private static final String DEFAULT_BRANCH = "develop";
  private static final AuthType AUTH_TYPE = AuthType.PAT;
  private static final String TOKEN_NAME = "token";
  private static final String USERNAME = "user";


  @Test
  public void testValidRepositoryConfig() {
    RepositoryConfig repo = new RepositoryConfig.Builder().setProvider(PROVIDER)
      .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AUTH_TYPE)
      .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();

    Assert.assertEquals(PROVIDER, repo.getProvider());
    Assert.assertEquals(LINK, repo.getLink());
    Assert.assertEquals(DEFAULT_BRANCH, repo.getDefaultBranch());
    Assert.assertEquals(AUTH_TYPE, repo.getAuth().getType());
    Assert.assertEquals(TOKEN_NAME, repo.getAuth().getTokenName());
    Assert.assertEquals(USERNAME, repo.getAuth().getUsername());
  }

  @Test
  public void testInvalidProvider() {

    try {
      new RepositoryConfig.Builder().setProvider(null)
        .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AUTH_TYPE)
        .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'provider' field cannot be null.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidLink() {

    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
        .setLink(null).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AUTH_TYPE)
        .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'link' field cannot be null.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidDefaultBranch() {

    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
        .setLink(LINK).setDefaultBranch(null).setAuthType(AUTH_TYPE)
        .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'defaultBranch' field cannot be null.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidAuthType() {

    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
        .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(null)
        .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'type' and 'tokenName' field in 'auth' object cannot be null.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testInvalidTokenName() {

    try {
      new RepositoryConfig.Builder().setProvider(PROVIDER)
        .setLink(LINK).setDefaultBranch(DEFAULT_BRANCH).setAuthType(AUTH_TYPE)
        .setTokenName(null).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals("'type' and 'tokenName' field in 'auth' object cannot be null.",
                          e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testMultipleInvalidFields() {

    try {
      new RepositoryConfig.Builder().setProvider(null)
        .setLink(null).setDefaultBranch(DEFAULT_BRANCH).setAuthType(null)
        .setTokenName(TOKEN_NAME).setUsername(USERNAME).build();
      Assert.fail();
    } catch (InvalidRepositoryConfigException e) {
      Assert.assertEquals(3, e.getFailures().size());
    }
  }
}
