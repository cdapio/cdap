/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 *
 */
public class FileContextLocationFactoryTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final String PATH_BASE = "/testing";

  private static MiniDFSCluster dfsCluster;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void init() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    dfsCluster.waitClusterUp();
    locationFactory = new FileContextLocationFactory(dfsCluster.getFileSystem().getConf(), PATH_BASE);
  }

  @AfterClass
  public static void finish() {
    dfsCluster.shutdown();
  }

  @Test
  public void testBasic() throws IOException {
    // Test basic location construction
    Assert.assertEquals(locationFactory.create("/file"), locationFactory.create("/file"));
    Assert.assertEquals(locationFactory.create("/file2"),
                        locationFactory.create(
                          URI.create(dfsCluster.getFileSystem().getScheme() + ":" + PATH_BASE + "/file2")));
    URI fsURI = dfsCluster.getFileSystem().getUri();
    Assert.assertEquals(locationFactory.create("/file3"),
                        locationFactory.create(
                          URI.create(String.format("%s://%s/%s/file3",
                                                   fsURI.getScheme(), fsURI.getAuthority(), PATH_BASE))));
    Assert.assertEquals(locationFactory.create("/"), locationFactory.create("/"));
    Assert.assertEquals(locationFactory.create("/"),
                        locationFactory.create(URI.create(dfsCluster.getFileSystem().getScheme() + ":" + PATH_BASE)));

    Assert.assertEquals(locationFactory.create("/"),
                        locationFactory.create(
                          URI.create(String.format("%s://%s/%s",
                                                   fsURI.getScheme(), fsURI.getAuthority(), PATH_BASE))));

    // Test file creation and rename
    Location location = locationFactory.create("/file");
    Assert.assertTrue(location.createNew());
    Assert.assertTrue(Locations.getParent(location).isDirectory());

    Location location2 = locationFactory.create("/file2");
    String message = "Testing Message";
    CharStreams.write(message, CharStreams.newWriterSupplier(Locations.newOutputSupplier(location2), Charsets.UTF_8));
    long length = location2.length();
    long lastModified = location2.lastModified();

    location2.renameTo(location);

    Assert.assertFalse(location2.exists());
    Assert.assertEquals(
      message,
      CharStreams.toString(CharStreams.newReaderSupplier(Locations.newInputSupplier(location), Charsets.UTF_8)));
    Assert.assertEquals(length, location.length());
    Assert.assertEquals(lastModified, location.lastModified());
  }

  @Test
  public void testList() throws IOException {
    Location dir = locationFactory.create("dir");

    // Check and create the directory
    Assert.assertFalse(dir.isDirectory());
    Assert.assertTrue(dir.mkdirs());
    Assert.assertTrue(dir.isDirectory());

    // Should have nothing inside
    Assert.assertTrue(dir.list().isEmpty());

    // Check and create a file inside the directory
    Location file = dir.append("file");
    Assert.assertFalse(file.isDirectory());
    Assert.assertTrue(file.createNew());
    Assert.assertFalse(file.isDirectory());

    // List on file should gives empty list
    Assert.assertTrue(file.list().isEmpty());

    // List on directory should gives the file inside
    List<Location> listing = dir.list();
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals(file, listing.get(0));

    // After deleting the file inside the directory, list on directory should be empty again.
    file.delete();
    Assert.assertTrue(dir.list().isEmpty());

    // List on a non-exist location would throw exception
    try {
      file.list();
      Assert.fail("List should fail on non-exist location.");
    } catch (IOException e) {
      // Expected
    }
  }
}
