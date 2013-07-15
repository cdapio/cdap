package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.data.dataset.DataSetTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * unit-test
 */
public class FileDataSetTest extends DataSetTestBase {
  @BeforeClass
  public static void configure() throws Exception {
    File file = File.createTempFile("fileDataSetTest", "foo");
    FileDataSet testFile = new FileDataSet("testFile", file.toURI());
    FileDataSet noFile1 = new FileDataSet("noFile1", new File(file.getAbsolutePath() + "_not_exist1").toURI());
    FileDataSet noFile2 = new FileDataSet("noFile2", new File(file.getAbsolutePath() + "_not_exist2").toURI());
    setupInstantiator(Arrays.<DataSet>asList(testFile, noFile1, noFile2));
  }

  @Test(expected = IOException.class)
  public void testCannotReadNonExistingFile() throws IOException {
    FileDataSet fileDataSet = instantiator.getDataSet("noFile1");
    fileDataSet.getInputStream();
  }

  @Test
  public void testCannotDeleteNonExistingFile() throws IOException {
    FileDataSet fileDataSet = instantiator.getDataSet("noFile1");
    Assert.assertFalse(fileDataSet.delete());
  }

  @Test
  public void testExists() throws IOException {
    Assert.assertFalse(((FileDataSet) instantiator.getDataSet("noFile1")).exists());
    Assert.assertTrue(((FileDataSet) instantiator.getDataSet("testFile")).exists());
  }

  @Test()
  public void testCanWriteToNonExistingFile() throws IOException {
    FileDataSet fileDataSet = instantiator.getDataSet("noFile2");
    // Writing some number
    OutputStream os = fileDataSet.getOutputStream();
    try {
      os.write(Bytes.toBytes(347L));
    } finally {
      os.close();
    }

    // Checking that data has changed
    InputStream is = fileDataSet.getInputStream();
    try {
      byte[] readBytes = new byte[1024];
      int length = is.read(readBytes);
      Assert.assertEquals(Bytes.SIZEOF_LONG, length);
      Assert.assertEquals(347L, Bytes.toLong(readBytes, 0, length));
    } finally {
      is.close();
    }
  }

  @Test
  public void testBasics() throws IOException {
    FileDataSet fileDataSet = instantiator.getDataSet("testFile");

    byte[] readBytes = new byte[1024];
    // File is empty in the beginning
    InputStream is = fileDataSet.getInputStream();
    try {
      Assert.assertEquals(-1, is.read(readBytes));
    } finally {
      is.close();
    }

    // Writing some number
    OutputStream os = fileDataSet.getOutputStream();
    try {
      os.write(Bytes.toBytes(55L));
    } finally {
      os.close();
    }

    // Checking that data has changed
    is = fileDataSet.getInputStream();
    try {
      int length = is.read(readBytes);
      Assert.assertEquals(Bytes.SIZEOF_LONG, length);
      Assert.assertEquals(55L, Bytes.toLong(readBytes, 0, length));
    } finally {
      is.close();
    }

    // Writing new data into a file (append not supported yet)
    os = fileDataSet.getOutputStream();
    try {
      os.write(Bytes.toBytes(23L));
    } finally {
      os.close();
    }

    // Checking that data has been appended
    is = fileDataSet.getInputStream();
    try {
      int length = is.read(readBytes);
      Assert.assertEquals(Bytes.SIZEOF_LONG, length);
      Assert.assertEquals(23L, Bytes.toLong(readBytes, 0, Bytes.SIZEOF_LONG));
    } finally {
      is.close();
    }
  }
}
