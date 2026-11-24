/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link FileContextProvider}.
 */
public class FileContextProviderTest {

  @Test
  public void testProvideWithCurrentAndConfiguredUser() {
    Configuration hConf = new Configuration();
    hConf.set("fs.defaultFS", "file:///");

    CConfiguration defaultConf = CConfiguration.create();
    FileContext defaultContext = new FileContextProvider(defaultConf, hConf).get();
    Assert.assertNotNull(defaultContext);

    CConfiguration remoteUserConf = CConfiguration.create();
    remoteUserConf.set(Constants.CFG_HDFS_USER, "cdap-remote-user");
    FileContext remoteContext = new FileContextProvider(remoteUserConf, hConf).get();
    Assert.assertNotNull(remoteContext);
    Assert.assertEquals("cdap-remote-user", remoteContext.getUgi().getShortUserName());
  }

  @Test
  public void testInvalidHdfsUserPropagatesUncheckedExceptionUnwrapped() {
    Configuration hConf = new Configuration();
    hConf.set("fs.defaultFS", "file:///");

    // Empty user string causes UserGroupInformation.createRemoteUser("") to throw
    // IllegalArgumentException, which createUGI() must rethrow unwrapped.
    CConfiguration invalidUserConf = CConfiguration.create();
    invalidUserConf.set(Constants.CFG_HDFS_USER, "");
    FileContextProvider provider = new FileContextProvider(invalidUserConf, hConf);

    try {
      provider.get();
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
