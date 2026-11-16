/*
 * Copyright © 2017-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.io;

import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for {@link Locations}.
 */
public class LocationsTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String TEST_BASE_PATH = "test_base";
  private static final String TEST_PATH = "some/test/path";

  @Test
  public void testParentHdfs() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, "abc");
    Location location = locationFactory.create("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void testRootParentFile() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, "abc");
    Location location = locationFactory.create("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void testRootParentLocal() throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(new File(File.separator));
    Location location = locationFactory.create("abc");
    location = location.append("def");
    Assert.assertEquals("def", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertEquals("abc", location.getName());
    location = Locations.getParent(location);
    Assert.assertNotNull(location);
    Assert.assertTrue(Locations.isRoot(location));
    Assert.assertTrue(location.getName().isEmpty());
    Assert.assertNull(Locations.getParent(location));
  }

  @Test
  public void absolutePathTests() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://1.2.3.4:8020/");
    LocationFactory locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    Location location1 = locationFactory.create(TEST_PATH);
    Location location2 = Locations.getLocationFromAbsolutePath(
        locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    locationFactory = new FileContextLocationFactory(conf, TEST_BASE_PATH);
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(
        locationFactory, location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());

    locationFactory = new LocalLocationFactory(new File(TEST_BASE_PATH));
    location1 = locationFactory.create(TEST_PATH);
    location2 = Locations.getLocationFromAbsolutePath(
        locationFactory, "////" + location1.toURI().getPath());
    Assert.assertEquals(location1.toURI(), location2.toURI());
  }

  @Test
  public void testLocationComparator() {
    LocationFactory locationFactory = new LocalLocationFactory(new File(File.separator));
    Object[][] testCases = new Object[][]{
        {"/parent1/child1", "/parent1/child1", 0},
        {"/parent1/child1/", "/parent1/child1/", 0},
        {"/parent1/child1", "/parent1/child1/", 0},
        {"/parent1/child1/", "/parent1/child1", 0},
        {"/parent1/child", "/parent1/child1", -1},
        {"/parent1/child1", "/parent1/child", 1}};

    for (Object[] testCase : testCases) {
      Location location1 = locationFactory.create((String) testCase[0]);
      Location location2 = locationFactory.create((String) testCase[1]);
      int expected = (int) testCase[2];
      Assert.assertEquals(expected, Locations.LOCATION_COMPARATOR.compare(location1, location2));
    }
  }

  @Test
  public void testInputAndOutputSuppliers() throws Exception {
    byte[] content = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

    // 1. LocalLocationFactory -> OutputSupplier and FileSeekableInputStream
    LocalLocationFactory localLf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location localLoc = localLf.create("local.bin");
    OutputSupplier<? extends OutputStream> outSupplier = Locations.newOutputSupplier(localLoc);
    try (OutputStream os = outSupplier.getOutput()) {
      os.write(content);
    }

    InputSupplier<? extends SeekableInputStream> localInSupplier =
        Locations.newInputSupplier(localLoc);
    try (SeekableInputStream sis = localInSupplier.getInput()) {
      Assert.assertEquals(content.length, sis.size());
      sis.seek(10L);
      Assert.assertEquals("abcdef",
          new String(ByteStreams.toByteArray(sis), StandardCharsets.UTF_8));
    }

    // 2. FileContextLocationFactory -> DFSSeekableInputStream (ownFileSystem = true)
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    File fcDir = TEMP_FOLDER.newFolder();
    FileContextLocationFactory fcLf = new FileContextLocationFactory(conf, fcDir.getAbsolutePath());
    Location fcLoc = fcLf.create("fc.bin");
    try (OutputStream os = Locations.newOutputSupplier(fcLoc).getOutput()) {
      os.write(content);
    }
    try (SeekableInputStream sis = Locations.newInputSupplier(fcLoc).getInput()) {
      Assert.assertEquals(content.length, sis.size());
      sis.seek(6L);
      Assert.assertEquals('6', (char) sis.read());
    }

    // 3. Locations.newInputSupplier(FileSystem, Path)
    FileSystem rawFs = FileSystem.getLocal(conf);
    Path hadoopPath = new Path(fcLoc.toURI());
    try (SeekableInputStream sis = Locations.newInputSupplier(rawFs, hadoopPath).getInput()) {
      Assert.assertEquals(content.length, sis.size());
      sis.seek(4L);
      Assert.assertEquals('4', (char) sis.read());
    }
  }

  @Test
  public void testNewInputSupplierErrorCleanupAndSizeFallback() throws Exception {
    // 1. Unsupported non-seekable InputStream whose close() also throws IOException:
    // Closeables.closeQuietly swallows close error and propagates the exact IOException unwrapped.
    AtomicBoolean nonSeekableClosed = new AtomicBoolean(false);
    Location unsupportedLoc = Mockito.mock(Location.class);
    Mockito.when(unsupportedLoc.getInputStream()).thenReturn(
        new ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)) {
          @Override
          public void close() throws IOException {
            nonSeekableClosed.set(true);
            throw new IOException("ignored close error");
          }
        });

    try {
      Locations.newInputSupplier(unsupportedLoc).getInput();
      Assert.fail("Expected IOException for non-seekable input stream");
    } catch (IOException e) {
      Assert.assertNull(e.getCause());
      Assert.assertTrue(e.getMessage().contains("Failed to create SeekableInputStream"));
    }
    Assert.assertTrue(nonSeekableClosed.get());

    // 2. Location returning FSDataInputStream whose getLocationFactory() throws RuntimeException
    // while input.close() throws IOException: input is closed quietly and cause is wrapped in IOException.
    AtomicBoolean fsInputClosed = new AtomicBoolean(false);
    FSDataInputStream dummyFsInput = new FSDataInputStream(
        new DummyFsInputStream(fsInputClosed, true));
    IllegalStateException factoryBoom = new IllegalStateException("location factory failure");
    Location brokenFactoryLoc = Mockito.mock(Location.class);
    Mockito.when(brokenFactoryLoc.getInputStream()).thenReturn(dummyFsInput);
    Mockito.when(brokenFactoryLoc.getLocationFactory()).thenThrow(factoryBoom);

    try {
      Locations.newInputSupplier(brokenFactoryLoc).getInput();
      Assert.fail("Expected IOException wrapping RuntimeException");
    } catch (IOException e) {
      Assert.assertSame(factoryBoom, e.getCause());
    }
    Assert.assertTrue(fsInputClosed.get());

    // 3. Locations.newInputSupplier(FileSystem, Path) when input.getWrappedStream() throws RuntimeException
    // and input.close() throws IOException.
    AtomicBoolean openFailedClosed = new AtomicBoolean(false);
    IllegalArgumentException wrappedBoom = new IllegalArgumentException("bad wrapped stream");
    FSDataInputStream brokenFsData = Mockito.mock(FSDataInputStream.class);
    Mockito.when(brokenFsData.getWrappedStream()).thenThrow(wrappedBoom);
    Mockito.doAnswer(inv -> {
      openFailedClosed.set(true);
      throw new IOException("close failure");
    }).when(brokenFsData).close();

    FileSystem mockFs = Mockito.mock(FileSystem.class);
    Path mockPath = new Path("/test/file");
    Mockito.when(mockFs.open(mockPath)).thenReturn(brokenFsData);
    try {
      Locations.newInputSupplier(mockFs, mockPath).getInput();
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      Assert.assertSame(wrappedBoom, e.getCause());
    }
    Assert.assertTrue(openFailedClosed.get());

    // 4. DistributedFileSystem with open file (isFileClosed == false) exercises
    // getFileLengthMethodSupplier reflection error path and returns -1L.
    DistributedFileSystem dfs = Mockito.mock(DistributedFileSystem.class);
    Mockito.when(dfs.isFileClosed(mockPath)).thenReturn(false);
    FSDataInputStream validFsData = new FSDataInputStream(
        new DummyFsInputStream(new AtomicBoolean(false), false));
    Mockito.when(dfs.open(mockPath)).thenReturn(validFsData);
    try (SeekableInputStream sis = Locations.newInputSupplier(dfs, mockPath).getInput()) {
      Assert.assertEquals(-1L, sis.size());
    }
  }

  @Test
  public void testUnpackTarAndTgzAndProcessLocations() throws IOException {
    // 1. Create a .tar archive and a .tar.gz archive with a directory entry and file entry.
    File tarFile = new File(TEMP_FOLDER.newFolder(), "bundle.tar");
    writeSampleTar(new FileOutputStream(tarFile));

    File tarTarget = TEMP_FOLDER.newFolder();
    Locations.unpack(Locations.toLocation(tarFile), tarTarget);
    Assert.assertTrue(new File(tarTarget, "subdir").isDirectory());
    Assert.assertEquals("hello tar",
        new String(Files.readAllBytes(new File(tarTarget, "hello.txt").toPath()),
            StandardCharsets.UTF_8));

    File tgzFile = new File(TEMP_FOLDER.newFolder(), "bundle.tar.gz");
    writeSampleTar(new GZIPOutputStream(new FileOutputStream(tgzFile)));
    File tgzTarget = TEMP_FOLDER.newFolder();
    Locations.unpack(Locations.toLocation(tgzFile), tgzTarget);
    Assert.assertEquals("hello tar",
        new String(Files.readAllBytes(new File(tgzTarget, "hello.txt").toPath()),
            StandardCharsets.UTF_8));

    // Plain .gz (not .tar.gz) should be rejected with IllegalArgumentException.
    File plainGz = new File(TEMP_FOLDER.newFolder(), "single.gz");
    Files.write(plainGz.toPath(), new byte[]{1, 2, 3});
    try {
      Locations.unpack(Locations.toLocation(plainGz), TEMP_FOLDER.newFolder());
      Assert.fail("Expected IllegalArgumentException for plain .gz");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    // 2. processLocations recursive vs non-recursive vs early stop.
    LocalLocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location root = lf.create("root");
    Locations.mkdirsIfNotExists(root);
    Location nestedDir = root.append("nested");
    Locations.mkdirsIfNotExists(nestedDir);
    try (OutputStream os = nestedDir.append("leaf.txt").getOutputStream()) {
      os.write("leaf".getBytes(StandardCharsets.UTF_8));
    }

    List<URI> recursiveUris = Locations.processLocations(root, true,
        new Processor<LocationStatus, List<URI>>() {
          private final List<URI> visited = new ArrayList<>();

          @Override
          public boolean process(LocationStatus input) {
            visited.add(input.getUri());
            return true;
          }

          @Override
          public List<URI> getResult() {
            return visited;
          }
        });
    Assert.assertEquals(3, recursiveUris.size());

    List<URI> nonRecursiveUris = Locations.processLocations(root, false,
        new Processor<LocationStatus, List<URI>>() {
          private final List<URI> visited = new ArrayList<>();

          @Override
          public boolean process(LocationStatus input) {
            visited.add(input.getUri());
            return true;
          }

          @Override
          public List<URI> getResult() {
            return visited;
          }
        });
    Assert.assertEquals(2, nonRecursiveUris.size());

    int earlyStopCount = Locations.processLocations(root, true,
        new Processor<LocationStatus, Integer>() {
          private int count;

          @Override
          public boolean process(LocationStatus input) {
            count++;
            return false;
          }

          @Override
          public Integer getResult() {
            return count;
          }
        });
    Assert.assertEquals(1, earlyStopCount);
  }

  private static void writeSampleTar(OutputStream rawOut) throws IOException {
    try (TarArchiveOutputStream tos = new TarArchiveOutputStream(rawOut)) {
      TarArchiveEntry dirEntry = new TarArchiveEntry("subdir/");
      tos.putArchiveEntry(dirEntry);
      tos.closeArchiveEntry();

      byte[] data = "hello tar".getBytes(StandardCharsets.UTF_8);
      TarArchiveEntry fileEntry = new TarArchiveEntry("hello.txt");
      fileEntry.setSize(data.length);
      tos.putArchiveEntry(fileEntry);
      tos.write(data);
      tos.closeArchiveEntry();
    }
  }

  private static final class DummyFsInputStream extends FSInputStream {
    private final AtomicBoolean closed;
    private final boolean failOnClose;
    private long pos;

    DummyFsInputStream(AtomicBoolean closed, boolean failOnClose) {
      this.closed = closed;
      this.failOnClose = failOnClose;
    }

    @Override
    public void seek(long newPos) {
      this.pos = newPos;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read() {
      return -1;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      if (failOnClose) {
        throw new IOException("dummy fs input close error");
      }
    }
  }
}
