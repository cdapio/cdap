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

package io.cdap.cdap.runtime.spi.runtimejob;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DataprocJobMain}.
 */
public class DataprocJobMainTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testAddJarUrlsSortsJarsDeterministicallyByName() throws Exception {
    File libDir = tempFolder.newFolder("lib");
    File zJar = new File(libDir, "z-last.jar");
    File aJar = new File(libDir, "a-first.jar");
    File nonJar = new File(libDir, "notes.txt");
    File mJar = new File(libDir, "m-middle.jar");
    File bJar = new File(libDir, "b-second.jar");

    Assert.assertTrue(zJar.createNewFile());
    Assert.assertTrue(aJar.createNewFile());
    Assert.assertTrue(nonJar.createNewFile());
    Assert.assertTrue(mJar.createNewFile());
    Assert.assertTrue(bJar.createNewFile());

    List<URL> jarUrls = new ArrayList<>();
    DataprocJobMain.addJarUrls(libDir, jarUrls);

    List<URL> expected = Arrays.asList(
        aJar.toURI().toURL(),
        bJar.toURI().toURL(),
        mJar.toURI().toURL(),
        zJar.toURI().toURL()
    );
    Assert.assertEquals(expected, jarUrls);

    List<URL> missingDirUrls = new ArrayList<>();
    DataprocJobMain.addJarUrls(new File(libDir, "does-not-exist"), missingDirUrls);
    Assert.assertTrue(missingDirUrls.isEmpty());
  }
}
