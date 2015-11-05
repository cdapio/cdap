/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bhooshanmogal on 11/5/15.
 */
public class SparkAppTableToRDD extends AbstractApplication {
  public static final String OUTPUT_DATASET_NAME = "output";
  public static final String INPUT_DATASET_NAME = "input";

  @Override
  public void configure() {
    createDataset(INPUT_DATASET_NAME, Table.class);
    createDataset(OUTPUT_DATASET_NAME, Table.class);
    addSpark(new JavaSparkTableToRDD());
  }

  public static class JavaSparkTableToRDD extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(JavaSparkTableToRDDProgram.class);
    }
  }

  public static class JavaSparkTableToRDDProgram implements JavaSparkProgram {

    @Override
    public void run(SparkContext context) {
      JavaPairRDD<byte[], Row> data = context.readFromDataset(INPUT_DATASET_NAME, byte[].class, Row.class);
      List<Map<String, String>> result = data.values().map(new Function<Row, Map<String, String>>() {
        @Override
        public Map<String, String> call(Row row) throws Exception {
          Map<String, String> map = new HashMap<>();
          for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
            map.put(new String(entry.getKey(), Charset.forName("UTF-8")),
                    new String(entry.getValue(), Charset.forName("UTF-8")));
          }
          return map;
        }
      }).collect();

      System.out.println(result);
//      org.apache.spark.SparkContext sc = context.getOriginalSparkContext();


      /*JavaPairRDD<byte[], Row> filtered = data.filter(new Function<Tuple2<byte[], Row>, Boolean>() {
        @Override
        public Boolean call(Tuple2<byte[], Row> v1) throws Exception {
          byte[] col3s = v1._2().get("col3");
          return col3s != null && Bytes.toString(col3s).equals("val13");
        }
      });
      JavaRDD<Tuple2<byte[], Row>> identity = data.map(new Function<Tuple2<byte[], Row>, Tuple2<byte[], Row>>() {
        @Override
        public Tuple2<byte[], Row> call(Tuple2<byte[], Row> v1) throws Exception {
          return v1;
        }
      });
      for (Tuple2<byte[], Row> rowTuple2 : identity.collect()) {
        System.out.println(Bytes.toString(rowTuple2._1()));
        System.out.println(Bytes.toString(rowTuple2._2().get("col1")));
        System.out.println(Bytes.toString(rowTuple2._2().get("col2")));
        System.out.println(Bytes.toString(rowTuple2._2().get("col3")));
      }*/
//      final List<Row> rows = new ArrayList<>();
//      Map<byte[], Row> rowMap = filtered.collectAsMap();
//      context.writeToDataset(data, OUTPUT_DATASET_NAME, byte[].class, byte[].class);
    }
  }
}
