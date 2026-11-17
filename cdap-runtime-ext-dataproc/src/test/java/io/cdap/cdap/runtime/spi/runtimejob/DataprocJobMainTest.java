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

package io.cdap.cdap.runtime.spi.runtimejob;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DataprocJobMain} classpath ordering.
 */
public class DataprocJobMainTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testAddJarUrlsSortsLexicographically() throws Exception {
    File libDir = tmpFolder.newFolder("lib");
    List<String> names = Arrays.asList(
        "org.apache.tephra.tephra-core-0.15.0-incubating.jar",
        "z-last.jar",
        "io.cdap.cdap.cdap-data-fabric-6.12.0-SNAPSHOT.jar",
        "a-first.jar");
    for (String name : names) {
      Assert.assertTrue(new File(libDir, name).createNewFile());
    }

    List<URL> urls = new ArrayList<>();
    DataprocJobMain.addJarUrls(libDir, urls);

    List<String> resolvedNames = urls.stream()
        .map(url -> new File(url.getPath()).getName())
        .collect(Collectors.toList());
    Assert.assertEquals(
        Arrays.asList(
            "a-first.jar",
            "io.cdap.cdap.cdap-data-fabric-6.12.0-SNAPSHOT.jar",
            "org.apache.tephra.tephra-core-0.15.0-incubating.jar",
            "z-last.jar"),
        resolvedNames);
  }
}
