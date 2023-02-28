/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for the {@link DirUtils} class.
 */
public class DirUtilsTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testDeleteDirectory() throws IOException {
    testDeleteDirectory(false);
    testDeleteDirectory(true);
  }

  private void testDeleteDirectory(boolean retain) throws IOException {
    // Create a directory. Inside, it has regular file, a regular directory, and a symlink to another directory.
    File dir = TEMP_FOLDER.newFolder();
    File otherDir = TEMP_FOLDER.newFolder();

    Files.createSymbolicLink(new File(dir, "symlink").toPath(), otherDir.toPath());
    Files.createDirectories(new File(dir, "dir").toPath());
    Files.createFile(new File(dir, "file").toPath());
    Files.createFile(new File(otherDir, "otherfile").toPath());

    DirUtils.deleteDirectoryContents(dir, retain);

    // The directory must be deleted
    Assert.assertEquals(retain, dir.isDirectory());
    if (retain) {
      Assert.assertEquals(0L, Files.list(dir.toPath()).count());
    }

    // Symlink target should get retained
    Assert.assertTrue(otherDir.isDirectory());
    Assert.assertTrue(new File(otherDir, "otherfile").exists());
  }
}
