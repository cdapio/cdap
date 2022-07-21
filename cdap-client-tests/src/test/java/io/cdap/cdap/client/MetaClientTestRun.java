/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ConfigEntry;
import io.cdap.cdap.proto.Version;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;

/**
 * Test for {@link MetaClient}.
 */
@Category(XSlowTests.class)
public class MetaClientTestRun extends ClientTestBase {

  @Test
  public void testAll() throws IOException, UnauthenticatedException, UnauthorizedException {
    MetaClient metaClient = new MetaClient(clientConfig);
    metaClient.ping();

    Version version = metaClient.getVersion();
    String expectedVersion = Resources.toString(Resources.getResource("VERSION"), Charsets.UTF_8).trim();
    Assert.assertEquals(expectedVersion, version.getVersion());

    // check a key that we know exists, to ensure that we retrieved the configurations
    Map<String, ConfigEntry> cdapConfig = metaClient.getCDAPConfig();
    ConfigEntry configEntry = cdapConfig.get(Constants.Dangerous.UNRECOVERABLE_RESET);
    Assert.assertNotNull(configEntry);
    Assert.assertEquals(Constants.Dangerous.UNRECOVERABLE_RESET, configEntry.getName());
    Assert.assertNotNull(configEntry.getValue());

    Map<String, ConfigEntry> hadoopConfig = metaClient.getHadoopConfig();
    configEntry = hadoopConfig.get("hadoop.tmp.dir");
    Assert.assertNotNull(configEntry);
    Assert.assertEquals("hadoop.tmp.dir", configEntry.getName());
    Assert.assertNotNull(configEntry.getValue());
  }
}
