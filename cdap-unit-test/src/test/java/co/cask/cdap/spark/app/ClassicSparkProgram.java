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

import co.cask.cdap.api.spark.JavaSparkMain;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.Arrays;

/**
 * A Spark program that has a static main method instead of extending from {@link JavaSparkMain}
 */
public class ClassicSparkProgram {

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    int result = jsc.parallelize(Arrays.asList(new MyInt(1), new MyInt(2), new MyInt(3), new MyInt(4), new MyInt(5)))
      .map(new Function<MyInt, Integer>() {
        @Override
        public Integer call(MyInt value) throws Exception {
          return value.toInt();
        }
      })
      .reduce(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
          return v1 + v2;
        }
      });

    if (result != 15) {
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
