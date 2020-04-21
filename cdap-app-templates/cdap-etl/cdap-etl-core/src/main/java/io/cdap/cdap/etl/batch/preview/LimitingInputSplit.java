/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.preview;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * An {@link InputSplit} that delegates to another {@link InputSplit} and also carries record limit information.
 */
public class LimitingInputSplit extends InputSplit implements Writable, Configurable {

  private List<InputSplit> inputSplits;
  private int recordLimit;
  private Configuration conf;
  private long length;
  private String[] locations;

  @SuppressWarnings("unused")
  public LimitingInputSplit() {
    // no-op, for deserialization
  }

  LimitingInputSplit(Configuration conf, List<InputSplit> inputSplits, int recordLimit) throws IOException {
    this.conf = conf;
    this.inputSplits = inputSplits;
    this.recordLimit = recordLimit;
    initialize();
  }

  private void initialize() throws IOException {
    try {
      long length = 0L;
      Set<String> locations = new LinkedHashSet<>();
      for (InputSplit split : inputSplits) {
        length += split.getLength();
        String[] splitLocations = split.getLocations();
        if (splitLocations != null) {
          locations.addAll(Arrays.asList(splitLocations));
        }
      }
      this.length = length;
      this.locations = locations.toArray(new String[0]);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted during initialization", e);
    }
  }

  List<InputSplit> getInputSplits() {
    return inputSplits;
  }

  int getRecordLimit() {
    return recordLimit;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String[] getLocations() {
    return locations;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(recordLimit);
    out.writeInt(inputSplits.size());
    for (InputSplit split : inputSplits) {
      Text.writeString(out, split.getClass().getName());
      Serializer serializer = new SerializationFactory(getConf()).getSerializer(split.getClass());
      serializer.open((OutputStream) out);
      //noinspection unchecked
      serializer.serialize(split);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    recordLimit = in.readInt();
    int size = in.readInt();
    List<InputSplit> splits = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      String className = Text.readString(in);
      try {
        Class<? extends InputSplit> cls = getConf().getClassLoader().loadClass(className).asSubclass(InputSplit.class);
        Deserializer deserializer = new SerializationFactory(getConf()).getDeserializer(cls);
        deserializer.open((InputStream) in);
        //noinspection unchecked
        splits.add((InputSplit) deserializer.deserialize(null));
      } catch (ClassNotFoundException e) {
        throw new IOException("Failed to load InputSplit class " + className, e);
      }
    }
    this.inputSplits = splits;
    initialize();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
