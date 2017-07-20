/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.data.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.TableSplit;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Unit test for the {@link Splits} class in API.
 */
public class SplitsTest {

  @Test
  public void testSerde() throws IOException, ClassNotFoundException {
    List<TableSplit> splits = Arrays.asList(
      new TableSplit(Bytes.toBytes("0"), Bytes.toBytes("1")),
      new TableSplit(Bytes.toBytes("1"), Bytes.toBytes("2"))
    );

    List<Split> decoded = Splits.decode(Splits.encode(splits), new ArrayList<Split>(), getClass().getClassLoader());
    Assert.assertEquals(splits, decoded);
  }

  @Test
  public void testOldSplit() throws IOException, ClassNotFoundException {
    // Test splits that doesn't override the
    List<OldSplit> splits = Arrays.asList(new OldSplit("1"), new OldSplit("2"));
    List<Split> decoded = Splits.decode(Splits.encode(splits), new ArrayList<Split>(), getClass().getClassLoader());
    Assert.assertEquals(splits, decoded);
  }

  @Test
  public void testNoDefaultCons() throws IOException, ClassNotFoundException {
    // Test splits that doesn't override the
    List<NewSplitNoDefaultCons> splits = Arrays.asList(new NewSplitNoDefaultCons("1"), new NewSplitNoDefaultCons("2"));
    List<Split> decoded = Splits.decode(Splits.encode(splits), new ArrayList<Split>(), getClass().getClassLoader());
    Assert.assertEquals(splits, decoded);
  }

  /**
   * A {@link Split} to simulate old split implementation.
   */
  public static final class OldSplit extends Split {
    private String start;

    public OldSplit() {
      this("0");
    }

    public OldSplit(String start) {
      this.start = start;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OldSplit that = (OldSplit) o;
      return Objects.equals(start, that.start);
    }

    @Override
    public int hashCode() {
      return Objects.hash(start);
    }
  }

  /**
   * A {@link Split} that implements the serialization methods, but don't have a default constructor
   */
  public static final class NewSplitNoDefaultCons extends Split {
    private String start;

    public NewSplitNoDefaultCons(String start) {
      this.start = start;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NewSplitNoDefaultCons that = (NewSplitNoDefaultCons) o;
      return Objects.equals(start, that.start);
    }

    @Override
    public int hashCode() {
      return Objects.hash(start);
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
      out.writeUTF(start);
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
      start = in.readUTF();
    }
  }
}
