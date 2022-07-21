/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Base class for authorization tests.
 */
public class AuthorizationTestBase {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  protected static final CConfiguration CCONF = CConfiguration.create();
  protected static final SConfiguration SCONF = SConfiguration.create();
  protected static final AuthorizationContextFactory AUTH_CONTEXT_FACTORY = new NoOpAuthorizationContextFactory();
  protected static final AuthenticationContext AUTH_CONTEXT = new AuthenticationTestContext();
  protected static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() throws IOException {
    CCONF.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    CCONF.setBoolean(Constants.Security.ENABLED, true);
    CCONF.setBoolean(Constants.Security.Authorization.ENABLED, true);
    CCONF.setBoolean(Constants.Metrics.AUTHORIZATION_METRICS_ENABLED, true);
    CCONF.setBoolean(Constants.Metrics.AUTHORIZATION_METRICS_TAGS_ENABLED, true);
    locationFactory = new LocalLocationFactory(TEMPORARY_FOLDER.newFolder());
  }
}
