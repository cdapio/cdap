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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.JavaSparkMain;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * A Spark program that has a static main method instead of extending from {@link JavaSparkMain}
 */
public class ClassicSparkProgram {

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    List<StructuredRecord> records = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      records.add(StructuredRecord
                    .builder(schema)
                    .set("name", "Name" + i)
                    .set("id", i)
                    .build());
    }

    // This test serialization of StructuredRecord as well as using custom kryo serializer
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    int result = jsc.parallelize(records).mapToPair(new PairFunction<StructuredRecord, MyInt, StructuredRecord>() {
      @Override
      public Tuple2<MyInt, StructuredRecord> call(StructuredRecord record) throws Exception {
        return new Tuple2<>(new MyInt((Integer) record.get("id")), record);
      }
    }).map(new Function<Tuple2<MyInt, StructuredRecord>, MyInt>() {
      @Override
      public MyInt call(Tuple2<MyInt, StructuredRecord> tuple) throws Exception {
        return tuple._1;
      }
    }).reduce(new Function2<MyInt, MyInt, MyInt>() {
      @Override
      public MyInt call(MyInt v1, MyInt v2) throws Exception {
        return new MyInt(v1.toInt() + v2.toInt());
      }
    }).toInt();

    if (result != 55) {
      throw new Exception("Expected result to be 55");
    }
  }

  public static final class MyInt {
    private final int i;

    public MyInt(int i) {
      this.i = i;
    }

    public int toInt() {
      return i;
    }
  }

  public static final class MyKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
      kryo.register(MyInt.class, new MyIntSerializer());
    }
  }

  public static final class MyIntSerializer extends Serializer<MyInt> {

    @Override
    public void write(Kryo kryo, Output output, MyInt object) {
      output.writeInt(object.toInt());
    }

    @Override
    public MyInt read(Kryo kryo, Input input, Class<MyInt> type) {
      return new MyInt(input.readInt());
    }
  }
}
