/*
 *  Copyright © 2023 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.runtime.spi;

import io.cdap.cdap.runtime.spi.runtimejob.DataprocJarUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataprocJarUtilTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testScalaLibraryNotPackaged() throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    LocalFile localFile = DataprocJarUtil.getTwillJar(locationFactory);
    File f = new File(localFile.getURI());

    Set<String> scalaLibraries = new HashSet<>();
    try (JarInputStream jarInput = new JarInputStream(Files.newInputStream(f.toPath()))) {
      JarEntry entry;
      while ((entry = jarInput.getNextJarEntry()) != null) {
        // CDAP-20674: when this test was added, scala-library and scala-parser-combinators were
        // both getting pulled into the twill.jar (with just scala-library causing problems)
        // It is possible for this test to start failing in the future if twill changes.
        // If that happens, it may be ok to add more scala libraries as expected libraries,
        // but it should be a conscious, tested, decision
        if (entry.getName().contains("scala") &&
            !entry.getName().contains("scala-parser-combinators")) {
          scalaLibraries.add(entry.getName());
        }
      }
    }
    Assert.assertTrue(String.format("Unexpected scala libraries in twill jar: %s",
        scalaLibraries.stream().collect(Collectors.joining())), scalaLibraries.isEmpty());
  }
}
