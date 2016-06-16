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

import co.cask.cdap.etl.batch.conversion.WritableConversions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Map output for which includes stageName and the record provided. This can be used to tag map output with stageName.
 * @param <RECORD>
 */
public class MapTaggedOutputWritable<RECORD> implements WritableComparable<MapTaggedOutputWritable> {
  private String stageName;
  private RECORD record;

  // required by Hadoop
  @SuppressWarnings("unused")
  public MapTaggedOutputWritable() {
  }

  public MapTaggedOutputWritable(String stageName, RECORD record) {
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

  // TODO change this - use stageNAme
  @Override
  public int compareTo(MapTaggedOutputWritable o) {

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
    MapTaggedOutputWritable that = (MapTaggedOutputWritable) o;
    return that.stageName.equals(stageName) && that.record.equals(record);
  }

  @Override
  public int hashCode() {
    return (record != null && stageName != null) ? record.hashCode() + stageName.hashCode() : 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableConversions.getConversion(stageName.getClass().getName()).toWritable(stageName).write(out);
    /* write class name of the record */
    WritableConversions.getConversion(String.class.getName()).toWritable(record.getClass().getName()).write(out);
    WritableConversions.getConversion(record.getClass().getName()).toWritable(record).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text stageWritable = new Text();
    stageWritable.readFields(in);
    stageName = stageWritable.toString();
    // read class name of the record
    Text classnameWritable = new Text();
    classnameWritable.readFields(in);
    String recordClassname = classnameWritable.toString();

    Writable recordNameWritable = WritableConversions.getConversion(recordClassname).toWritable();
    recordNameWritable.readFields(in);
    record = (RECORD) WritableConversions.getConversion(recordClassname).fromWritable(recordNameWritable);
  }
}
