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

package co.cask.cdap.etl.batch.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Map output for which includes stageName and the record provided. This can be used to tag map output with stageName.
 * @param <RECORD> Writable record to be serialized along with stageName
 */
public class TaggedWritable<RECORD extends Writable> implements
  WritableComparable<TaggedWritable<RECORD>>, Configurable {
  private String stageName;
  private RECORD record;
  private Configuration conf;
  private ObjectWritable recordWritable;

  // required by Hadoop
  @SuppressWarnings("unused")
  public TaggedWritable() {
  }

  public TaggedWritable(String stageName, RECORD record) {
    this.stageName = stageName;
    this.record = record;
  }

  public String getStageName() {
    return stageName;
  }

  public RECORD getRecord() {
    return record;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public void setRecord(RECORD record) {
    this.record = record;
  }

  @Override
  public int compareTo(TaggedWritable o) {
    return Integer.compare(hashCode(), o.hashCode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaggedWritable that = (TaggedWritable) o;
    return that.stageName.equals(stageName) && that.record.equals(record);
  }

  @Override
  public int hashCode() {
    return (record != null && stageName != null) ? record.hashCode() + stageName.hashCode() : 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, stageName);
    ObjectWritable recordWritable = new ObjectWritable(record);
    recordWritable.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.stageName = Text.readString(in);
    this.recordWritable.readFields(in);
    this.record = (RECORD) recordWritable.get();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    recordWritable = new ObjectWritable();
    // ObjectWritable does not set conf while reading fields
    recordWritable.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
