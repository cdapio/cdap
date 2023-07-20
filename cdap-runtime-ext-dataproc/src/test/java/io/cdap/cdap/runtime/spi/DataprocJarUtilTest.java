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
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
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

    try (JarInputStream jarInput = new JarInputStream(Files.newInputStream(f.toPath()))) {
      JarEntry entry;
      while ((entry = jarInput.getNextJarEntry()) != null) {
        Assert.assertFalse(entry.getName().contains("scala-library"));
      }
    }
  }
}
