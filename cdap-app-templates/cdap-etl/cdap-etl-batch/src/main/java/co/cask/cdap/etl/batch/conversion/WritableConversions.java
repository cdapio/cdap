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

package co.cask.cdap.etl.batch.conversion;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.batch.StructuredRecordWritable;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Functions to convert common classes to their WritableComparable equivalent and vice versa.
 */
public class WritableConversions {
  // classname -> function for functions that transform common classes that should be transformed into classes usable
  // as mapper keys or values.
  private static final Map<String, WritableConversion<?, ?>> CONVERSIONS;

  static {
    ImmutableMap.Builder<String, WritableConversion<?, ?>> builder = ImmutableMap.builder();
    builder.put(StructuredRecord.class.getName(),
                new WritableConversion<StructuredRecord, StructuredRecordWritable>() {
                  @Override
                  public StructuredRecordWritable toWritable(StructuredRecord val) {
                    return new StructuredRecordWritable(val);
                  }

                  @Override
                  public StructuredRecord fromWritable(StructuredRecordWritable val) {
                    return val.get();
                  }
                });
    builder.put(String.class.getName(),
                new WritableConversion<String, Text>() {
                  @Override
                  public Text toWritable(String val) {
                    return new Text(val);
                  }

                  @Override
                  public String fromWritable(Text val) {
                    return val.toString();
                  }
                });
    builder.put(Integer.class.getName(),
                new WritableConversion<Integer, IntWritable>() {
                  @Override
                  public IntWritable toWritable(Integer val) {
                    return new IntWritable(val);
                  }

                  @Override
                  public Integer fromWritable(IntWritable val) {
                    return val.get();
                  }
                });
    builder.put(Long.class.getName(),
                new WritableConversion<Long, LongWritable>() {
                  @Override
                  public LongWritable toWritable(Long val) {
                    return new LongWritable(val);
                  }

                  @Override
                  public Long fromWritable(LongWritable val) {
                    return val.get();
                  }
                });
    builder.put(Float.class.getName(),
                new WritableConversion<Float, FloatWritable>() {
                  @Override
                  public FloatWritable toWritable(Float val) {
                    return new FloatWritable(val);
                  }

                  @Override
                  public Float fromWritable(FloatWritable val) {
                    return val.get();
                  }
                });
    builder.put(Double.class.getName(),
                new WritableConversion<Double, DoubleWritable>() {
                  @Override
                  public DoubleWritable toWritable(Double val) {
                    return new DoubleWritable(val);
                  }

                  @Override
                  public Double fromWritable(DoubleWritable val) {
                    return val.get();
                  }
                });
    builder.put(Boolean.class.getName(),
                new WritableConversion<Boolean, BooleanWritable>() {
                  @Override
                  public BooleanWritable toWritable(Boolean val) {
                    return new BooleanWritable(val);
                  }

                  @Override
                  public Boolean fromWritable(BooleanWritable val) {
                    return val.get();
                  }
                });
    builder.put(byte[].class.getName(),
                new WritableConversion<byte[], BytesWritable>() {
                  @Override
                  public BytesWritable toWritable(byte[] val) {
                    return new BytesWritable(val);
                  }

                  @Override
                  public byte[] fromWritable(BytesWritable val) {
                    return val.getBytes();
                  }
                });
    CONVERSIONS = builder.build();
  }

  /**
   * Get the conversion functions to and from the WritableComparable for the specified class.
   * Returns null if none exists.
   *
   * @param className the name of the class to convert to/from a WritableComparable
   * @return conversion functions to and from the WritableComparable for the specified class
   */
  @Nullable
  public static <KEY, VAL extends Writable> WritableConversion<KEY, VAL> getConversion(String className) {
    //noinspection unchecked
    return (WritableConversion<KEY, VAL>) CONVERSIONS.get(className);
  }

  private WritableConversions() {
    // no-op to prevent instantiation of a helper class
  }
}
