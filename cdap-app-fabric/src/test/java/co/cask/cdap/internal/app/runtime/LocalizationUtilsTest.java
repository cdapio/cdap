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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.base.Charsets;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

/**
 * Tests for {@link LocalizationUtils}.
 */
public class LocalizationUtilsTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testRemoteFile() throws IOException {
    File directory = TEMP_FOLDER.newFolder("ftp");
    File ftpFile = new File(directory, "ftp_file");
    String ftpFileContents = "Contents of ftp_file";
    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(ftpFile.getAbsolutePath(), ftpFileContents));

    String user = "user";
    String password = "password";
    FakeFtpServer ftpServer = new FakeFtpServer();
    // Use any available port
    ftpServer.setServerControlPort(0);
    ftpServer.addUserAccount(new UserAccount(user, password, directory.getAbsolutePath()));
    ftpServer.setFileSystem(fileSystem);
    ftpServer.start();

    try {
      URI uri = URI.create(String.format("ftp://%s:%s@localhost:%d/%s", user, password,
                                         ftpServer.getServerControlPort(), ftpFile.getName()));
      File localizationDir = TEMP_FOLDER.newFolder("localRemote");
      File localizedResource = LocalizationUtils.localizeResource("file1", new LocalizeResource(uri, false),
                                                                  localizationDir);
      Assert.assertTrue(localizedResource.exists());
      Assert.assertTrue(localizedResource.isFile());
      Assert.assertEquals(ftpFileContents, com.google.common.io.Files.toString(localizedResource, Charsets.UTF_8));
    } finally {
      ftpServer.stop();
    }
  }

  @Test
  public void testZip() throws IOException {
    String zipFileName = "target";
    File directory = TEMP_FOLDER.newFolder("zip");
    File file1 = File.createTempFile("file1", ".txt", directory);
    File file2 = File.createTempFile("file2", ".txt", directory);
    File zipFile = createZipFile(zipFileName, directory, false);
    File localizationDir = TEMP_FOLDER.newFolder("localZip");
    File localizedResource = LocalizationUtils.localizeResource(zipFileName, new LocalizeResource(zipFile, true),
                                                                localizationDir);
    Assert.assertTrue(localizedResource.isDirectory());
    File[] files = localizedResource.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(2, files.length);
    if (file1.getName().equals(files[0].getName())) {
      Assert.assertEquals(file2.getName(), files[1].getName());
    } else {
      Assert.assertEquals(file1.getName(), files[1].getName());
      Assert.assertEquals(file2.getName(), files[0].getName());
    }
  }

  @Test
  public void testJar() throws IOException {
    String jarFileName = "target";
    File directory = TEMP_FOLDER.newFolder("jar");
    File libDir = new File(directory, "lib");
    Assert.assertTrue(libDir.mkdirs());
    File someClassFile = File.createTempFile("SomeClass", ".class", directory);
    File someOtherClassFile = File.createTempFile("SomeOtherClass", ".class", directory);
    File jarFile = createZipFile(jarFileName, directory, true);
    File localizationDir = TEMP_FOLDER.newFolder("localJar");
    File localizedResource = LocalizationUtils.localizeResource(jarFileName, new LocalizeResource(jarFile, true),
                                                                localizationDir);
    Assert.assertTrue(localizedResource.isDirectory());
    File[] files = localizedResource.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(3, files.length);
    for (File file : files) {
      String name = file.getName();
      if (libDir.getName().equals(name)) {
        Assert.assertTrue(file.isDirectory());
      } else {
        Assert.assertTrue(someClassFile.getName().equals(name) || someOtherClassFile.getName().equals(name));
      }
    }
  }

  @Test
  public void testTar() throws IOException {
    testTarFiles(TarFileType.TAR);
  }

  @Test
  public void testTarGz() throws IOException {
    testTarFiles(TarFileType.TAR_GZ);
  }

  @Test
  public void testTgz() throws IOException {
    testTarFiles(TarFileType.TGZ);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGz() throws IOException {
    String gzFileName = "target";
    File directory = TEMP_FOLDER.newFolder("gz");
    File source = File.createTempFile("source", ".txt", directory);
    File gzFile = createGzFile(gzFileName, source);
    File localizationDir = TEMP_FOLDER.newFolder("localGz");
    LocalizationUtils.localizeResource(gzFileName, new LocalizeResource(gzFile, true), localizationDir);
  }

  private void testTarFiles(TarFileType type) throws IOException {
    String tarFileName = "target";
    // Have to use short file/directory names because TarArchiveOutputStream does not like long paths.
    File directory;
    File localizationDir;
    switch (type) {
      case TAR:
        directory = TEMP_FOLDER.newFolder("t1");
        localizationDir = TEMP_FOLDER.newFolder("localTar");
        break;
      case TAR_GZ:
        directory = TEMP_FOLDER.newFolder("t2");
        localizationDir = TEMP_FOLDER.newFolder("localTarGz");
        break;
      case TGZ:
        directory = TEMP_FOLDER.newFolder("t3");
        localizationDir = TEMP_FOLDER.newFolder("localTgz");
        break;
      default:
        throw new IllegalArgumentException("Unexpected type: " + type);
    }
    File file1 = new File(Files.createFile(Paths.get(new File(directory, "f1").toURI())).toUri());
    File file2 = new File(Files.createFile(Paths.get(new File(directory, "f2").toURI())).toUri());
    File tarFile;
    switch (type) {
      case TAR:
        tarFile = createTarFile(tarFileName, file1, file2);
        break;
      case TAR_GZ:
        tarFile = createTarGzFile(tarFileName, file1, file2);
        break;
      case TGZ:
        tarFile = createTgzFile(tarFileName, file1, file2);
        break;
      default:
        throw new IllegalArgumentException("Unexpected type: " + type);
    }
    File localizedResource = LocalizationUtils.localizeResource(tarFileName, new LocalizeResource(tarFile, true),
                                                                localizationDir);
    Assert.assertTrue(localizedResource.isDirectory());
    File[] files = localizedResource.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(2, files.length);
    for (File file : files) {
      String name = file.getName();
      Assert.assertTrue(file1.getName().equals(name) || file2.getName().equals(name));
    }
  }

  private File createZipFile(String zipFileName, File dirToJar, boolean isJar) throws IOException {
    String extension = isJar ? ".jar" : ".zip";
    File target = TEMP_FOLDER.newFile(zipFileName + extension);
    BundleJarUtil.createJar(dirToJar, target);
    File[] files = dirToJar.listFiles();
    Assert.assertNotNull(files);
    for (File file : files) {
      if (!file.isDirectory()) {
        BundleJarUtil.getEntry(Locations.toLocation(target), file.getName()).getInput().close();
      }
    }
    return target;
  }

  private File createTarFile(String tarFileName, File ... filesToAdd) throws IOException {
    File target = TEMP_FOLDER.newFile(tarFileName + ".tar");
    try (TarArchiveOutputStream tos =
           new TarArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(target)))) {
      addFilesToTar(tos, filesToAdd);
    }
    return target;
  }

  private File createTgzFile(String tgzFileName, File ... filesToAdd) throws IOException {
    File target = TEMP_FOLDER.newFile(tgzFileName + ".tgz");
    try (TarArchiveOutputStream tos =
           new TarArchiveOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(target))))) {
      addFilesToTar(tos, filesToAdd);
    }
    return target;
  }

  private File createTarGzFile(String tarGzFileName, File ... filesToAdd) throws IOException {
    File target = TEMP_FOLDER.newFile(tarGzFileName + ".tar.gz");
    try (TarArchiveOutputStream tos =
           new TarArchiveOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(target))))) {
      addFilesToTar(tos, filesToAdd);
    }
    return target;
  }

  private void addFilesToTar(TarArchiveOutputStream tos, File ... filesToAdd) throws IOException {
    for (File file : filesToAdd) {
      TarArchiveEntry tarEntry = new TarArchiveEntry(file);
      tos.putArchiveEntry(tarEntry);
      if (file.isFile()) {
        com.google.common.io.Files.copy(file, tos);
      }
      tos.closeArchiveEntry();
    }
  }

  private File createGzFile(String gzFileName, File sourceFile) throws IOException {
    File target = TEMP_FOLDER.newFile(gzFileName + ".gz");
    try (GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(target))) {
      com.google.common.io.Files.copy(sourceFile, gzos);
    }
    return target;
  }

  private enum TarFileType {
    TAR,
    TAR_GZ,
    TGZ
  }
}
