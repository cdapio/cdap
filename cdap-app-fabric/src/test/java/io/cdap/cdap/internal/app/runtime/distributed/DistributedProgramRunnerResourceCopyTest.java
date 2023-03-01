/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import java.io.File;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DistributedProgramRunnerResourceCopyTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCopyRuntimeTokenWithoutTokenFileDoesNotCopy() throws Exception {
    File tempDirCopyTo = temporaryFolder.newFolder();
    File tempDirCopyFrom = temporaryFolder.newFolder();

    Map<String, String> arguments = new HashMap<>();
    arguments.put(ProgramOptionConstants.PEER_NAME, "test-peer");
    arguments.put(ProgramOptionConstants.PROGRAM_RESOURCE_URI, tempDirCopyFrom.toURI().toString());

    LocationFactory locationFactory = new LocalLocationFactory();
    Map<String, LocalizeResource> localizeResourceMap = new HashMap<>();
    DistributedProgramRunner.copyRuntimeToken(locationFactory, new BasicArguments(arguments), tempDirCopyTo,
                                              localizeResourceMap);
    Assert.assertFalse(localizeResourceMap.containsKey(Constants.Security.Authentication.RUNTIME_TOKEN_FILE));
  }

  @Test
  public void testCopyRuntimeTokenWithTokenFileSuccess() throws Exception {
    File tempDirCopyTo = temporaryFolder.newFolder();
    File tempDirCopyFrom = temporaryFolder.newFolder();
    File runtimeTokenFile = new File(tempDirCopyFrom, Constants.Security.Authentication.RUNTIME_TOKEN_FILE);
    byte[] runtimeTokenBytes = new byte[64];
    SecureRandom.getInstanceStrong().nextBytes(runtimeTokenBytes);
    Files.write(runtimeTokenFile.toPath(), runtimeTokenBytes);

    Map<String, String> arguments = new HashMap<>();
    arguments.put(ProgramOptionConstants.PEER_NAME, "test-peer");
    arguments.put(ProgramOptionConstants.PROGRAM_RESOURCE_URI, tempDirCopyFrom.toURI().toString());

    LocationFactory locationFactory = new LocalLocationFactory(temporaryFolder.getRoot());
    Map<String, LocalizeResource> localizeResourceMap = new HashMap<>();
    DistributedProgramRunner.copyRuntimeToken(locationFactory, new BasicArguments(arguments), tempDirCopyTo,
                                              localizeResourceMap);
    Assert.assertTrue(localizeResourceMap.containsKey(Constants.Security.Authentication.RUNTIME_TOKEN_FILE));
    File copiedRuntimeTokenFile = new File(localizeResourceMap
                                             .get(Constants.Security.Authentication.RUNTIME_TOKEN_FILE).getURI());
    Assert.assertArrayEquals(Files.readAllBytes(copiedRuntimeTokenFile.toPath()), runtimeTokenBytes);
  }
}
