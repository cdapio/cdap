/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * An {@link InputSplit} that tags another InputSplit with extra data.
 */
public abstract class TaggedInputSplit extends InputSplit implements Configurable, Writable {

  private InputSplit inputSplit;
  private Configuration conf;

  TaggedInputSplit() { }

  /**
   * Creates a new TaggedInputSplit.
   *
   * @param inputSplit The InputSplit to be tagged
   * @param conf The configuration to use
   */
  TaggedInputSplit(InputSplit inputSplit, Configuration conf) {
    this.inputSplit = inputSplit;
    this.conf = conf;
  }


  /**
   * Implemented by subclasses to deserialize additional fields from this TaggedInputSplit.
   * Note that the order of fields read must be the same as they are written
   * by {@link #writeAdditionalFields(DataOutput)}
   *
   * @param in the DataInput to read the fields from
   * @throws IOException
   */
  protected abstract void readAdditionalFields(DataInput in) throws IOException;

  /**
   * Implemented by subclasses to serialize additional fields to this TaggedInputSplit.
   * Note that the order of fields written must be the same as they are read
   * by {@link #readAdditionalFields(DataInput)}.
   *
   * @param out the DataOutput to write the fields to
   * @throws IOException
   */
  protected abstract void writeAdditionalFields(DataOutput out) throws IOException;

  /**
   * Retrieves the original InputSplit.
   *
   * @return The InputSplit that was tagged
   */
  public InputSplit getInputSplit() {
    return inputSplit;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return inputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return inputSplit.getLocations();
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void readFields(DataInput in) throws IOException {
    Class<? extends InputSplit> inputSplitClass = (Class<? extends InputSplit>) readClass(in);
    readAdditionalFields(in);
    inputSplit = ReflectionUtils.newInstance(inputSplitClass, conf);
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer deserializer = factory.getDeserializer(inputSplitClass);
    deserializer.open((DataInputStream) in);
    inputSplit = (InputSplit) deserializer.deserialize(inputSplit);
  }

  Class<?> readClass(DataInput in) throws IOException {
    String className = StringInterner.weakIntern(Text.readString(in));
    try {
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void write(DataOutput out) throws IOException {
    Class<? extends InputSplit> inputSplitClass = inputSplit.getClass();
    Text.writeString(out, inputSplitClass.getName());
    writeAdditionalFields(out);
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = factory.getSerializer(inputSplitClass);
    serializer.open((DataOutputStream) out);
    serializer.serialize(inputSplit);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return inputSplit.toString();
  }

}
