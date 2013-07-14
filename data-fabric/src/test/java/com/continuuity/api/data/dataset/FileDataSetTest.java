package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data.operation.StatusCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;

public class FileDataSetTest extends DataSetTestBase {
  @BeforeClass
  public static void configure() throws Exception {
    File file = File.createTempFile("fileDataSetTest", "foo");
    FileDataSet testFile = new FileDataSet("testFile", file.getAbsolutePath());
    FileDataSet noFile1 = new FileDataSet("noFile1", file.getAbsolutePath() + "_not_exist1");
    FileDataSet noFile2 = new FileDataSet("noFile2", file.getAbsolutePath() + "_not_exist2");
    setupInstantiator(Arrays.<DataSet>asList(testFile, noFile1, noFile2));
  }

  @Test(expected = IOException.class)
  public void testCannotReadNonExistingFile() throws IOException {
    FileDataSet fileDataSet = instantiator.getDataSet("noFile1");
    fileDataSet.getInputStream();
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
