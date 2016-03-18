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
import com.google.common.base.Function;
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
  private static final Map<String, WritableConversion<?, ?>> conversions =
    ImmutableMap.<String, WritableConversion<?, ?>>builder()
      .put(StructuredRecord.class.getName(), new WritableConversion<StructuredRecord, StructuredRecordWritable>() {
        @Override
        public Class<StructuredRecordWritable> getWritableClass() {
          return StructuredRecordWritable.class;
        }

        @Override
        public Function<StructuredRecord, StructuredRecordWritable> getToWritableFunction() {
          return new Function<StructuredRecord, StructuredRecordWritable>() {
            @Nullable
            @Override
            public StructuredRecordWritable apply(StructuredRecord input) {
              return new StructuredRecordWritable(input);
            }
          };
        }

        @Override
        public Function<StructuredRecordWritable, StructuredRecord> getFromWritableFunction() {
          return new Function<StructuredRecordWritable, StructuredRecord>() {
            @Nullable
            @Override
            public StructuredRecord apply(@Nullable StructuredRecordWritable input) {
              return input == null ? null : input.get();
            }
          };
        }
      })
    .put(String.class.getName(), new WritableConversion<String, Text>() {
      @Override
      public Class<Text> getWritableClass() {
        return Text.class;
      }

      @Override
      public Function<String, Text> getToWritableFunction() {
        return new Function<String, Text>() {
          @Nullable
          @Override
          public Text apply(@Nullable String input) {
            return input == null ? null : new Text(input);
          }
        };
      }

      @Override
      public Function<Text, String> getFromWritableFunction() {
        return new Function<Text, String>() {
          @Nullable
          @Override
          public String apply(@Nullable Text input) {
            return input == null ? null : input.toString();
          }
        };
      }
    })
    .put(Integer.class.getName(), new WritableConversion<Integer, IntWritable>() {
      @Override
      public Class<IntWritable> getWritableClass() {
        return IntWritable.class;
      }

      @Override
      public Function<Integer, IntWritable> getToWritableFunction() {
        return new Function<Integer, IntWritable>() {
          @Nullable
          @Override
          public IntWritable apply(@Nullable Integer input) {
            return input == null ? null : new IntWritable(input);
          }
        };
      }

      @Override
      public Function<IntWritable, Integer> getFromWritableFunction() {
        return new Function<IntWritable, Integer>() {
          @Nullable
          @Override
          public Integer apply(@Nullable IntWritable input) {
            return input == null ? null : input.get();
          }
        };
      }
    })
    .put(Long.class.getName(), new WritableConversion<Long, LongWritable>() {
      @Override
      public Class<LongWritable> getWritableClass() {
        return LongWritable.class;
      }

      @Override
      public Function<Long, LongWritable> getToWritableFunction() {
        return new Function<Long, LongWritable>() {
          @Nullable
          @Override
          public LongWritable apply(@Nullable Long input) {
            return input == null ? null : new LongWritable(input);
          }
        };
      }

      @Override
      public Function<LongWritable, Long> getFromWritableFunction() {
        return new Function<LongWritable, Long>() {
          @Nullable
          @Override
          public Long apply(@Nullable LongWritable input) {
            return input == null ? null : input.get();
          }
        };
      }
    })
    .put(Double.class.getName(), new WritableConversion<Double, DoubleWritable>() {
      @Override
      public Class<DoubleWritable> getWritableClass() {
        return DoubleWritable.class;
      }

      @Override
      public Function<Double, DoubleWritable> getToWritableFunction() {
        return new Function<Double, DoubleWritable>() {
          @Nullable
          @Override
          public DoubleWritable apply(@Nullable Double input) {
            return input == null ? null : new DoubleWritable(input);
          }
        };
      }

      @Override
      public Function<DoubleWritable, Double> getFromWritableFunction() {
        return new Function<DoubleWritable, Double>() {
          @Nullable
          @Override
          public Double apply(@Nullable DoubleWritable input) {
            return input == null ? null : input.get();
          }
        };
      }
    })
    .put(Float.class.getName(), new WritableConversion<Float, FloatWritable>() {
      @Override
      public Class<FloatWritable> getWritableClass() {
        return FloatWritable.class;
      }

      @Override
      public Function<Float, FloatWritable> getToWritableFunction() {
        return new Function<Float, FloatWritable>() {
          @Nullable
          @Override
          public FloatWritable apply(@Nullable Float input) {
            return input == null ? null : new FloatWritable(input);
          }
        };
      }

      @Override
      public Function<FloatWritable, Float> getFromWritableFunction() {
        return new Function<FloatWritable, Float>() {
          @Nullable
          @Override
          public Float apply(@Nullable FloatWritable input) {
            return input == null ? null : input.get();
          }
        };
      }
    })
    .put(Boolean.class.getName(), new WritableConversion<Boolean, BooleanWritable>() {
      @Override
      public Class<BooleanWritable> getWritableClass() {
        return BooleanWritable.class;
      }

      @Override
      public Function<Boolean, BooleanWritable> getToWritableFunction() {
        return new Function<Boolean, BooleanWritable>() {
          @Nullable
          @Override
          public BooleanWritable apply(@Nullable Boolean input) {
            return input == null ? null : new BooleanWritable(input);
          }
        };
      }

      @Override
      public Function<BooleanWritable, Boolean> getFromWritableFunction() {
        return new Function<BooleanWritable, Boolean>() {
          @Nullable
          @Override
          public Boolean apply(@Nullable BooleanWritable input) {
            return input == null ? null : input.get();
          }
        };
      }
    })
    .put(byte[].class.getName(), new WritableConversion<byte[], BytesWritable>() {
      @Override
      public Function<byte[], BytesWritable> getToWritableFunction() {
        return new Function<byte[], BytesWritable>() {
          @Nullable
          @Override
          public BytesWritable apply(@Nullable byte[] input) {
            return input == null ? null : new BytesWritable(input);
          }
        };
      }

      @Override
      public Function<BytesWritable, byte[]> getFromWritableFunction() {
        return new Function<BytesWritable, byte[]>() {
          @Nullable
          @Override
          public byte[] apply(@Nullable BytesWritable input) {
            return input == null ? null : input.getBytes();
          }
        };
      }
    })
    .build();

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
    return (WritableConversion<KEY, VAL>) conversions.get(className);
  }

  private WritableConversions() {
    // no-op to prevent instantiation of a helper class
  }
}
