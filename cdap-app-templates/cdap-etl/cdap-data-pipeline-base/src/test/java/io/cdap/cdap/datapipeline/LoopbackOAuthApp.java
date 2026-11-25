/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline;

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.datapipeline.oauth.OAuthUrlValidator;
import io.cdap.cdap.datapipeline.service.OAuthHandler;

import java.net.InetAddress;

/**
 * Test-only application deploying an {@link OAuthHandler} subclass whose URL validator allows
 * loopback HTTP calls to an in-process mock OAuth server. Production {@link OAuthHandler} has
 * no configuration flag permitting loopback or plain HTTP URLs.
 */
public class LoopbackOAuthApp extends AbstractApplication<Config> {
  static final String SERVICE_NAME = "loopbackOAuth";

  @Override
  public void configure() {
    setName("loopbackOAuthApp");
    addService(new LoopbackOAuthService());
  }

  /**
   * Test-only system service hosting {@link LoopbackOAuthHandler}.
   */
  public static final class LoopbackOAuthService extends AbstractSystemService {
    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      addHandler(new LoopbackOAuthHandler());
      createTable(OAuthStore.TABLE_SPEC);
    }
  }

  /**
   * Test-only {@link OAuthHandler} allowing loopback HTTP calls for mock token server tests.
   */
  public static final class LoopbackOAuthHandler extends OAuthHandler {
    @Override
    protected OAuthUrlValidator createUrlValidator() {
      return OAuthUrlValidator.forTesting(false, true, InetAddress::getAllByName);
    }
  }
}
