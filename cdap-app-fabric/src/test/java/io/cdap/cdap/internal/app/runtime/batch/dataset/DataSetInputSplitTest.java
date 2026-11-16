/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch.dataset;

import io.cdap.cdap.api.data.batch.Split;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DataSetInputSplit}.
 */
public class DataSetInputSplitTest {

  /**
   * Custom serializable {@link Split} implementation for round-trip testing.
   */
  public static final class SampleSplit extends Split {

    private String partitionId;
    private long byteLength;

    public SampleSplit() {
    }

    public SampleSplit(String partitionId, long byteLength) {
      this.partitionId = partitionId;
      this.byteLength = byteLength;
    }

    @Override
    public long getLength() {
      return byteLength;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
      out.writeUTF(partitionId);
      out.writeLong(byteLength);
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
      this.partitionId = in.readUTF();
      this.byteLength = in.readLong();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SampleSplit)) {
        return false;
      }
      SampleSplit that = (SampleSplit) other;
      return byteLength == that.byteLength && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitionId, byteLength);
    }
  }

  @Test
  public void testDataInputOutputRoundTripAndNullContextClassLoaderFallback() throws Exception {
    SampleSplit original = new SampleSplit("part-07", 4096L);
    DataSetInputSplit inputSplit = new DataSetInputSplit(original);

    Assert.assertEquals(original, inputSplit.getSplit());
    Assert.assertEquals(4096L, inputSplit.getLength());
    Assert.assertEquals(0, inputSplit.getLocations().length);

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
      inputSplit.write(dataOut);
    }
    byte[] serialized = byteOut.toByteArray();

    DataSetInputSplit restored = new DataSetInputSplit();
    try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(serialized))) {
      restored.readFields(dataIn);
    }
    Assert.assertEquals(original, restored.getSplit());
    Assert.assertEquals(4096L, restored.getLength());

    // Verify fallback to getClass().getClassLoader() when thread context ClassLoader is null
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(serialized))) {
      DataSetInputSplit nullContextRestored = new DataSetInputSplit();
      nullContextRestored.readFields(dataIn);
      Assert.assertEquals(original, nullContextRestored.getSplit());
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  @Test
  public void testReadFieldsWrapsClassNotFoundExceptionInRuntimeException() throws Exception {
    SampleSplit original = new SampleSplit("part-99", 128L);
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (DataOutputStream dataOut = new DataOutputStream(byteOut)) {
      new DataSetInputSplit(original).write(dataOut);
    }

    ClassNotFoundException expectedCnf = new ClassNotFoundException("missing SampleSplit class");
    ClassLoader failingLoader = new ClassLoader(null) {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (SampleSplit.class.getName().equals(name)) {
          throw expectedCnf;
        }
        return DataSetInputSplitTest.class.getClassLoader().loadClass(name);
      }
    };

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(failingLoader);
    try (DataInputStream dataIn =
        new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()))) {
      DataSetInputSplit split = new DataSetInputSplit();
      try {
        split.readFields(dataIn);
        Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(expectedCnf, thrown.getCause());
      }
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
