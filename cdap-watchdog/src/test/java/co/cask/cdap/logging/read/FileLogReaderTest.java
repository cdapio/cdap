/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

public class FileLogReaderTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testGetFilesInRange() throws Exception {
    Location base = new LocalLocationFactory().create(tempFolder.newFolder().toURI());
    NavigableMap<Long, Location> sortedFiles = new TreeMap<>(ImmutableSortedMap.of(10L, base.append("10"),
                                                                                   15L, base.append("15"),
                                                                                   20L, base.append("20"),
                                                                                   28L, base.append("28"),
                                                                                   35L, base.append("35")));

    Assert.assertEquals(Collections.<Location>emptyList(), FileLogReader.getFilesInRange(sortedFiles, 1, 10));

    Assert.assertEquals(ImmutableList.of(base.append("10")), FileLogReader.getFilesInRange(sortedFiles, 1, 11));

    // Since we don't know the last log entry in 35, we need to return 35 for query with [46, 50)
    Assert.assertEquals(ImmutableList.of(base.append("35")), FileLogReader.getFilesInRange(sortedFiles, 46, 50));

    Assert.assertEquals(ImmutableList.of(base.append("15"),
                                         base.append("20")),
                        FileLogReader.getFilesInRange(sortedFiles, 15, 28));

    Assert.assertEquals(ImmutableList.of(base.append("10"),
                                         base.append("15"),
                                         base.append("20"),
                                         base.append("28")),
                        FileLogReader.getFilesInRange(sortedFiles, 13, 30));

    Assert.assertEquals(ImmutableList.of(base.append("10"),
                                         base.append("15"),
                                         base.append("20"),
                                         base.append("28")),
                        FileLogReader.getFilesInRange(sortedFiles, 10, 34));

    Assert.assertEquals(ImmutableList.of(base.append("10"),
                                         base.append("15"),
                                         base.append("20"),
                                         base.append("28")),
                        FileLogReader.getFilesInRange(sortedFiles, 11, 35));

    Assert.assertEquals(ImmutableList.of(base.append("10"),
                                         base.append("15"),
                                         base.append("20"),
                                         base.append("28"),
                                         base.append("35")),
                        FileLogReader.getFilesInRange(sortedFiles, 11, 36));

    Assert.assertEquals(ImmutableList.of(base.append("20"),
                                         base.append("28")),
                        FileLogReader.getFilesInRange(sortedFiles, 25, 32));
  }
}
