/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.Version;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Test for {@link MetaClient}.
 */
@Category(XSlowTests.class)
public class MetaClientTestRun extends ClientTestBase {

  @Test
  public void testAll() throws IOException, UnAuthorizedAccessTokenException {
    MetaClient metaClient = new MetaClient(clientConfig);
    metaClient.ping();
    Version version = metaClient.getVersion();
    String expectedVersion = Resources.toString(Resources.getResource("VERSION"), Charsets.UTF_8).trim();
    Assert.assertEquals(expectedVersion, version.getVersion());
  }
}
