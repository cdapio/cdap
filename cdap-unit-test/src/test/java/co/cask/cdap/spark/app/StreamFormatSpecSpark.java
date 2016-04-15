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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.stream.GenericStreamEventData;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Spark program for testing stream format specification usage.
 */
public class StreamFormatSpecSpark extends AbstractSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setMainClass(StreamFormatSpecSpark.class);
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    SQLContext sqlContext = new SQLContext(jsc);

    // Read from CSV stream and turn it into a DataFrame
    String streamName = sec.getRuntimeArguments().get("stream.name");
    Schema schema = Schema.recordOf("record", ImmutableList.of(
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("age", Schema.of(Schema.Type.INT))
    ));
    FormatSpecification formatSpec = new FormatSpecification("csv", schema);
    JavaPairRDD<Long, GenericStreamEventData<StructuredRecord>> rdd =
      sec.fromStream(streamName, formatSpec, StructuredRecord.class);

    JavaRDD<Person> personRDD = rdd.values().map(new Function<GenericStreamEventData<StructuredRecord>, Person>() {
      @Override
      public Person call(GenericStreamEventData<StructuredRecord> data) throws Exception {
        StructuredRecord record = data.getBody();
        return new Person(record.<String>get("name"), record.<Integer>get("age"));
      }
    });

    sqlContext.createDataFrame(personRDD, Person.class).registerTempTable("people");

    // Execute a SQL on the table and save the result
    JavaPairRDD<String, Integer> resultRDD = sqlContext.sql(sec.getRuntimeArguments().get("sql.statement"))
      .toJavaRDD()
      .mapToPair(new PairFunction<Row, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(Row row) throws Exception {
          return new Tuple2<>(row.getString(0), row.getInt(1));
        }
      });

    sec.saveAsDataset(resultRDD, sec.getRuntimeArguments().get("output.dataset"));
  }

  public static class Person implements Serializable {
    private String name;
    private int age;

    public Person() {
      // For serialization
    }

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public int getAge() {
      return age;
    }
  }
}
