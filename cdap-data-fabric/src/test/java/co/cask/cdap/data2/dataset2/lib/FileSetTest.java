/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.file.FileSetDataset;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Maps;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

  static FileSet fileSet1;
  static FileSet fileSet2;
  private static final Id.Namespace OTHER_NAMESPACE = Id.Namespace.from("yourspace");
  private static final Id.DatasetInstance testFileSetInstance1 =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "testFileSet");
  private static final Id.DatasetInstance testFileSetInstance2 =
    Id.DatasetInstance.from(OTHER_NAMESPACE, "testFileSet");
  private static final Id.DatasetInstance testFileSetInstance3 =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "absoluteFileSet");
  private static final Id.DatasetInstance testFileSetInstance4 =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "lookAlikeFileSet");
  private static final Id.DatasetInstance testFileSetInstance5 =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "externalFileSet");

  @Before
  public void before() throws Exception {
    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance1, FileSetProperties.builder()
      .setBasePath("testDir").build());
    Map<String, String> fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File1");
    FileSetArguments.setOutputPath(fileArgs, "some?File1");
    fileSet1 = dsFrameworkUtil.getInstance(testFileSetInstance1, fileArgs);

    dsFrameworkUtil.createInstance("fileSet", testFileSetInstance2, FileSetProperties.builder()
      .setBasePath("testDir").build());
    fileArgs = Maps.newHashMap();
    FileSetArguments.setInputPath(fileArgs, "some?File2");
    FileSetArguments.setOutputPath(fileArgs, "some?File2");
    fileSet2 = dsFrameworkUtil.getInstance(testFileSetInstance2, fileArgs);
  }

  @After
  public void after() throws Exception {
    deleteInstance(testFileSetInstance1);
    deleteInstance(testFileSetInstance2);
    deleteInstance(testFileSetInstance3);
    deleteInstance(testFileSetInstance4);
    deleteInstance(testFileSetInstance5);
  }

  static void deleteInstance(Id.DatasetInstance id) throws Exception {
    if (dsFrameworkUtil.getInstance(id) != null) {
      dsFrameworkUtil.deleteInstance(id);
    }
  }

  @Test
  public void testWriteRead() throws IOException {
    Location fileSet1Output = fileSet1.getOutputLocation();
    Location fileSet2Output = fileSet2.getOutputLocation();
    Location fileSet1NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet1Output)));
    Location fileSet2NsDir = Locations.getParent(Locations.getParent(Locations.getParent(fileSet2Output)));
    Assert.assertNotNull(fileSet1NsDir);
    Assert.assertNotNull(fileSet2NsDir);
    Assert.assertEquals(fileSet1NsDir.getName(), DatasetFrameworkTestUtil.NAMESPACE_ID.getId());
    Assert.assertEquals(fileSet2NsDir.getName(), OTHER_NAMESPACE.getId());

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
                                   Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "badFileSet"),
                                   FileSetProperties.builder().setBasePath(absolutePath).build());
  }

  @Test(expected = DataSetException.class)
  public void testAbsolutePathInsideCDAPDouble() throws IOException, DatasetManagementException {
    // test that it rejects also paths that have // in them
    String absolutePath = dsFrameworkUtil.getConfiguration()
      .get(Constants.CFG_LOCAL_DATA_DIR).replace("/", "//").concat("/hello");
    dsFrameworkUtil.createInstance("fileSet",
                                   Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "badFileSet"),
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
   public void testRollback() throws IOException, TransactionFailureException {
    // test deletion of an empty output directory
    Location outputLocation = fileSet1.getOutputLocation();
    Assert.assertFalse(outputLocation.exists());

    Assert.assertTrue(outputLocation.mkdirs());

    Assert.assertTrue(outputLocation.exists());
    ((FileSetDataset) fileSet1).onFailure();

    Assert.assertFalse(outputLocation.exists());
  }

  @Test
  public void testRollbackOfNonDirectoryOutput() throws IOException, TransactionFailureException {
    // test deletion of an output location, pointing to a non-directory file
    Location outputFile = fileSet1.getOutputLocation();
    Assert.assertFalse(outputFile.exists());

    outputFile.getOutputStream().close();

    Assert.assertTrue(outputFile.exists());
    ((FileSetDataset) fileSet1).onFailure();

    // the output file should still not be deleted
    Assert.assertTrue(outputFile.exists());
  }

  @Test
  public void testRollbackWithNonEmptyDir() throws IOException, TransactionFailureException {
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
