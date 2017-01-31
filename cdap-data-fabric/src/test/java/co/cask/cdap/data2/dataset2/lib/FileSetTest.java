/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib;

import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.file.FileSetDataset;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class FileSetTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final NamespaceId OTHER_NAMESPACE = new NamespaceId("yourspace");
  private static final DatasetId testFileSetInstance1 =
    DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testFileSet");
  private static final DatasetId testFileSetInstance2 = OTHER_NAMESPACE.dataset("testFileSet");
  private static final DatasetId testFileSetInstance3 =
    DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("absoluteFileSet");
  private static final DatasetId testFileSetInstance4 =
    DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("lookAlikeFileSet");
  private static final DatasetId testFileSetInstance5 =
    DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("externalFileSet");
  private static final DatasetId testFileSetInstance6 =
    DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("nonExternalFileSet1");

  @After
  public void after() throws Exception {
    dsFrameworkUtil.getFramework().deleteAllInstances(DatasetFrameworkTestUtil.NAMESPACE_ID);
    dsFrameworkUtil.getFramework().deleteAllInstances(OTHER_NAMESPACE);
  }

  // helper to create a commonly used file set instance and return its Java instance.
  private FileSet createFileset(DatasetId dsid) throws IOException, DatasetManagementException {
    dsFrameworkUtil.createInstance("fileSet", dsid, FileSetProperties.builder()
      .setBasePath("testDir").build());
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File1");
    FileSetArguments.setOutputPath(fileArgs, "some?File1");
    return dsFrameworkUtil.getInstance(dsid, fileArgs);
  }

  @Test
  public void testWriteRead() throws IOException, DatasetManagementException {
    FileSet fileSet1 = createFileset(testFileSetInstance1);
    FileSet fileSet2 = createFileset(testFileSetInstance2);
    Location fileSet1Output = fileSet1.getOutputLocation();
    Location fileSet2Output = fileSet2.getOutputLocation();
    Location fileSet1NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet1Output)));
    Location fileSet2NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet2Output)));
    Assert.assertNotNull(fileSet1NsDir);
    Assert.assertNotNull(fileSet2NsDir);
    Assert.assertEquals(fileSet1NsDir.getName(), DatasetFrameworkTestUtil.NAMESPACE_ID.getNamespace());
    Assert.assertEquals(fileSet2NsDir.getName(), OTHER_NAMESPACE.getNamespace());

    Assert.assertNotEquals(fileSet1.getInputLocations().get(0).toURI().getPath(),
                           fileSet2.getInputLocations().get(0).toURI().getPath());
    Assert.assertNotEquals(fileSet1Output.toURI().getPath(), fileSet2Output.toURI().getPath());

    try (OutputStream out = fileSet1.getOutputLocation().getOutputStream()) {
      out.write(42);
    }
    try (OutputStream out = fileSet2.getOutputLocation().getOutputStream()) {
      out.write(54);
    }

    try (InputStream in = fileSet1.getInputLocations().get(0).getInputStream()) {
      Assert.assertEquals(42, in.read());
    }
    try (InputStream in = fileSet2.getInputLocations().get(0).getInputStream()) {
      Assert.assertEquals(54, in.read());
    }
  }

  @Test
  public void testPermissions() throws Exception {

    String fsPermissions = "rwxrwx--x";
    String customPermissions = "rwx--x--x";
    String group = UserGroupInformation.getCurrentUser().getPrimaryGroupName();

    // create one file set with default permission so that the namespace data dir exists
    dsFrameworkUtil.createInstance("fileSet", OTHER_NAMESPACE.dataset("dummy"), DatasetProperties.EMPTY);

    // determine the default permissions of created directories (we want to test with different perms)
    String defaultPermissions = dsFrameworkUtil.getInjector().getInstance(NamespacedLocationFactory.class)
      .get(OTHER_NAMESPACE.toId()).getPermissions();
    if (fsPermissions.equals(defaultPermissions)) {
      // swap the permissions so we can test with different file set permissions than the default
      customPermissions = "rwxrwx--x";
      fsPermissions = "rwx--x--x";
    }

    // create a dataset with configured permissions that are different from the default
    DatasetId datasetId = OTHER_NAMESPACE.dataset("testPermFS");
    dsFrameworkUtil.createInstance("fileSet", datasetId, FileSetProperties.builder()
      .setBasePath("perm/test/path")
      .add(DatasetProperties.PROPERTY_PERMISSIONS, fsPermissions)
      .add(DatasetProperties.PROPERTY_PERMISSIONS_GROUP, group)
      .build());
    FileSet fs = dsFrameworkUtil.getInstance(datasetId);

    // validate that the entire hierarchy of directories was created with the correct permissions
    Location base = fs.getBaseLocation();
    Assert.assertEquals(group, base.getGroup());
    Assert.assertEquals(fsPermissions, base.getPermissions());
    Location parent = Locations.getParent(base);
    Assert.assertNotNull(parent);
    Assert.assertEquals(group, parent.getGroup());
    Assert.assertEquals(fsPermissions, parent.getPermissions());
    parent = Locations.getParent(parent);
    Assert.assertNotNull(parent);
    Assert.assertEquals(group, parent.getGroup());
    Assert.assertEquals(fsPermissions, parent.getPermissions());
    Location nsRoot = Locations.getParent(parent);
    Assert.assertNotNull(nsRoot);
    Assert.assertNotEquals(fsPermissions, nsRoot.getPermissions());

    // create an empty file and validate it is created with the fileset's permissions
    Location child = base.append("a");
    Location grandchild = child.append("b");
    grandchild.getOutputStream().close();
    Assert.assertEquals(group, child.getGroup());
    Assert.assertEquals(group, grandchild.getGroup());
    Assert.assertEquals(fsPermissions, child.getPermissions());
    Assert.assertEquals(fsPermissions, grandchild.getPermissions());

    // create an empty file with custom permissions and validate them
    child = base.append("x");
    grandchild = child.append("y");
    grandchild.getOutputStream(customPermissions).close();
    Assert.assertEquals(group, child.getGroup());
    Assert.assertEquals(group, grandchild.getGroup());
    Assert.assertEquals(customPermissions, child.getPermissions());
    Assert.assertEquals(customPermissions, grandchild.getPermissions());

    // instantiate the dataset with custom permissions in the runtime arguments
    fs = dsFrameworkUtil.getInstance(datasetId, ImmutableMap.of(
      DatasetProperties.PROPERTY_PERMISSIONS, customPermissions));

    // create an empty file with custom permissions and validate them
    base = fs.getBaseLocation();
    child = base.append("p");
    grandchild = child.append("q");
    grandchild.getOutputStream().close();
    Assert.assertEquals(group, child.getGroup());
    Assert.assertEquals(group, grandchild.getGroup());
    Assert.assertEquals(customPermissions, child.getPermissions());
    Assert.assertEquals(customPermissions, grandchild.getPermissions());
  }

  @Test
  public void testAbsolutePath() throws IOException, DatasetManagementException {
    String absolutePath = tmpFolder.newFolder() + "/absolute/path";
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance3, FileSetProperties.builder()
      .setBasePath(absolutePath).build());
    // validate that the base path for the file set was created
    Assert.assertTrue(new File(absolutePath).isDirectory());

    // instantiate the file set with an output path
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setOutputPath(fileArgs, "out");
    FileSet fileSet = dsFrameworkUtil.getInstance(testFileSetInstance3, fileArgs);

    // write to the output path
    Assert.assertEquals(absolutePath + "/out", fileSet.getOutputLocation().toURI().getPath());
    try (OutputStream out = fileSet.getOutputLocation().getOutputStream()) {
      out.write(42);
    }

    // validate that the file was created
    Assert.assertTrue(new File(absolutePath + "/out").isFile());
  }

  @Test(expected = DataSetException.class)
  public void testAbsolutePathInsideCDAP() throws IOException, DatasetManagementException {
    String absolutePath = dsFrameworkUtil.getConfiguration().get(Constants.CFG_LOCAL_DATA_DIR).concat("/hello");
    dsFrameworkUtil.createInstance("fileSet",
                                   DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("badFileSet"),
                                   FileSetProperties.builder().setBasePath(absolutePath).build());
  }

  @Test(expected = DataSetException.class)
  public void testAbsolutePathInsideCDAPDouble() throws IOException, DatasetManagementException {
    // test that it rejects also paths that have // in them
    String absolutePath = dsFrameworkUtil.getConfiguration()
      .get(Constants.CFG_LOCAL_DATA_DIR).replace("/", "//").concat("/hello");
    dsFrameworkUtil.createInstance("fileSet",
                                   DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("badFileSet"),
                                   FileSetProperties.builder().setBasePath(absolutePath).build());
  }

  @Test
  public void testAbsolutePathLooksLikeCDAP() throws IOException, DatasetManagementException {
    String absolutePath = dsFrameworkUtil.getConfiguration().get(Constants.CFG_LOCAL_DATA_DIR).concat("-hello");
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance4,
                                   FileSetProperties.builder().setBasePath(absolutePath).build());
  }

  @Test
  public void testExternalAbsolutePath() throws IOException, DatasetManagementException {
    // create an external dir and create a file in it
    String absolutePath = tmpFolder.newFolder() + "/absolute/path";
    File absoluteFile = new File(absolutePath);
    absoluteFile.mkdirs();
    File someFile = new File(absoluteFile, "some.file");
    someFile.createNewFile();

    // create an external dataset
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(absolutePath)
                                     .setDataExternal(true)
                                     .build());

    // instantiate the file set with an input and output path
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some.file");
    FileSetArguments.setOutputPath(fileArgs, "out");
    FileSet fileSet = dsFrameworkUtil.getInstance(testFileSetInstance5, fileArgs);
    Assert.assertNotNull(fileSet);

    // read the existing file
    Location input = fileSet.getInputLocations().iterator().next();
    InputStream in = input.getInputStream();
    in.close();

    // attempt to write an output file
    try {
      fileSet.getOutputLocation();
      Assert.fail("Extrernal file set should not allow writing output.");
    } catch (UnsupportedOperationException e) {
      // expected
    }

    // delete the dataset and validate that the files are still there
    dsFrameworkUtil.deleteInstance(testFileSetInstance5);
    Assert.assertTrue(someFile.exists());
  }

  @Test(expected = IOException.class)
  public void testExternalNonExistentPath() throws IOException, DatasetManagementException {
    // create an external dir and create a file in it
    String absolutePath = tmpFolder.newFolder() + "/not/there";
    // attempt to create an external dataset - should fail
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(absolutePath)
                                     .setDataExternal(true)
                                     .build());
  }

  @Test
  public void testNonExternalExistentPath() throws Exception {
    // Create an instance at a location
    String absolutePath = tmpFolder.newFolder() + "/some/existing/location";
    File file = new File(absolutePath);
    Assert.assertTrue(file.mkdirs());
    // Try to add another instance of non external fileset at the same location
    try {
      dsFrameworkUtil.createInstance("fileSet", testFileSetInstance6,
                                     FileSetProperties.builder()
                                       .setBasePath(absolutePath)
                                       .setDataExternal(false)
                                       .build());
      Assert.fail("Expected IOException from createInstance()");
    } catch (IOException e) {
      // expected
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReuseAndExternal() throws IOException, DatasetManagementException {
    dsFrameworkUtil.createInstance("fileSet",
                                   DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("badFileSet"),
                                   FileSetProperties.builder()
                                     .setDataExternal(true)
                                     .setUseExisting(true)
                                     .build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPossessAndExternal() throws IOException, DatasetManagementException {
    dsFrameworkUtil.createInstance("fileSet",
                                   DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("badFileSet"),
                                   FileSetProperties.builder()
                                     .setDataExternal(true)
                                     .setPossessExisting(true)
                                     .build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPossessAndReuse() throws IOException, DatasetManagementException {
    dsFrameworkUtil.createInstance("fileSet",
                                   DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("badFileSet"),
                                   FileSetProperties.builder()
                                     .setUseExisting(true)
                                     .setPossessExisting(true)
                                     .build());
  }

  @Test(expected = IOException.class)
  public void testReuseNonExistentPath() throws IOException, DatasetManagementException {
    // create an external dir and create a file in it
    String absolutePath = tmpFolder.newFolder() + "/not/there";
    // attempt to create an external dataset - should fail
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(absolutePath)
                                     .setUseExisting(true)
                                     .build());
  }

  @Test(expected = IOException.class)
  public void testPossessNonExistentPath() throws IOException, DatasetManagementException {
    // create an external dir and create a file in it
    String absolutePath = tmpFolder.newFolder() + "/not/there";
    // attempt to create an external dataset - should fail
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(absolutePath)
                                     .setPossessExisting(true)
                                     .build());
  }

  @Test
  public void testReuseDoesNotDelete() throws IOException, DatasetManagementException {
    String existingPath = tmpFolder.newFolder() + "/existing/path";
    File existingDir = new File(existingPath);
    existingDir.mkdirs();
    File someFile = new File(existingDir, "some.file");
    someFile.createNewFile();

    // create an external dataset
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(existingPath)
                                     .setUseExisting(true)
                                     .build());
    Assert.assertTrue(someFile.exists());

    // truncate the file set
    dsFrameworkUtil.getFramework().truncateInstance(testFileSetInstance5);
    Assert.assertTrue(someFile.exists());

    // truncate the file set
    dsFrameworkUtil.getFramework().deleteInstance(testFileSetInstance5);
    Assert.assertTrue(someFile.exists());
  }

  @Test
  public void testPossessDoesDelete() throws IOException, DatasetManagementException {
    String existingPath = tmpFolder.newFolder() + "/existing/path";
    File existingDir = new File(existingPath);
    existingDir.mkdirs();
    File someFile = new File(existingDir, "some.file");
    someFile.createNewFile();

    // create an external dataset
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance5,
                                   FileSetProperties.builder()
                                     .setBasePath(existingPath)
                                     .setPossessExisting(true)
                                     .build());
    Assert.assertTrue(someFile.exists());

    // truncate the file set
    dsFrameworkUtil.getFramework().truncateInstance(testFileSetInstance5);
    Assert.assertFalse(someFile.exists());
    Assert.assertTrue(existingDir.exists());
    someFile.createNewFile();

    // truncate the file set
    dsFrameworkUtil.getFramework().deleteInstance(testFileSetInstance5);
    Assert.assertFalse(someFile.exists());
    Assert.assertFalse(existingDir.exists());
  }

  @Test
   public void testRollback() throws IOException, TransactionFailureException, DatasetManagementException {
    // test deletion of an empty output directory
    FileSet fileSet1 = createFileset(testFileSetInstance1);
    Location outputLocation = fileSet1.getOutputLocation();
    Assert.assertFalse(outputLocation.exists());

    Assert.assertTrue(outputLocation.mkdirs());

    Assert.assertTrue(outputLocation.exists());
    ((FileSetDataset) fileSet1).onFailure();

    Assert.assertFalse(outputLocation.exists());
  }

  @Test
  public void testRollbackOfNonDirectoryOutput()
    throws IOException, TransactionFailureException, DatasetManagementException {
    // test deletion of an output location, pointing to a non-directory file
    FileSet fileSet1 = createFileset(testFileSetInstance1);
    Location outputFile = fileSet1.getOutputLocation();
    Assert.assertFalse(outputFile.exists());

    outputFile.getOutputStream().close();

    Assert.assertTrue(outputFile.exists());
    ((FileSetDataset) fileSet1).onFailure();

    // the output file should still not be deleted
    Assert.assertTrue(outputFile.exists());
  }

  @Test
  public void testRollbackWithNonEmptyDir()
    throws IOException, TransactionFailureException, DatasetManagementException {
    FileSet fileSet1 = createFileset(testFileSetInstance1);
    Location outputDir = fileSet1.getOutputLocation();
    Assert.assertFalse(outputDir.exists());

    Assert.assertTrue(outputDir.mkdirs());

    Location outputFile = outputDir.append("outputFile");
    // this will create the outputFile
    outputFile.getOutputStream().close();

    Assert.assertTrue(outputFile.exists());

    Assert.assertTrue(outputDir.exists());
    ((FileSetDataset) fileSet1).onFailure();

    // both the output dir and file in it should still exist
    Assert.assertTrue(outputDir.exists());
    Assert.assertTrue(outputFile.exists());
  }
}
