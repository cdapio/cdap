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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.Splits;
import co.cask.cdap.api.dataset.Dataset;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class for {@link InputSplit} of a {@link Dataset}.
 *
 * @see InputSplit
 */
public class DataSetInputSplit extends InputSplit implements Writable {
  private Split split;

  public DataSetInputSplit() {
  }

  public DataSetInputSplit(final Split split) {
    this.split = split;
  }

  public Split getSplit() {
    return split;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return split.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    // By default splits locations are not provided (todo: fix)
    return new String[0];
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    Splits.serialize(split, out);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(final DataInput in) throws IOException {
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = getClass().getClassLoader();
      }
      split = Splits.deserialize(in, classLoader);
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }
}
