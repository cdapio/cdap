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

package org.example.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Common class for word count spark plugins.
 */
public class WordCount implements Serializable {
  private static final Pattern WHITESPACE = Pattern.compile("\\s");
  private final String field;

  public WordCount(String field) {
    this.field = field;
  }

  public void validateSchema(Schema inputSchema) {
    // a null input schema means its unknown until runtime, or its not constant
    if (inputSchema != null) {
      // if the input schema is constant and known at configure time, check that the input field exists and is a string.
      Schema.Field inputField = inputSchema.getField(field);
      if (inputField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in input schema %s.", field, inputSchema));
      }
      Schema fieldSchema = inputField.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (fieldType != Schema.Type.STRING) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of illegal type %s. Must be of type %s.",
                        field, fieldType, Schema.Type.STRING));
      }
    }
  }

  public JavaPairRDD<String, Long> countWords(JavaRDD<StructuredRecord> input) {
    return input.flatMap(new SplitFunction(field))
      .groupBy(new Identity<String>())
      .flatMapToPair(new CountFunction());
  }

  private static class Identity<T> implements Function<T, T> {
    @Override
    public T call(T t) throws Exception {
      return t;
    }
  }

  private static class SplitFunction implements FlatMapFunction<StructuredRecord, String> {
    private final String field;

    public SplitFunction(String field) {
      this.field = field;
    }

    @Override
    public Iterable<String> call(StructuredRecord record) throws Exception {
      String val = record.get(field);
      List<String> words = new ArrayList<>();
      if (val != null) {
        Collections.addAll(words, WHITESPACE.split(val));
      }
      return words;
    }
  }

  private static class CountFunction implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, Long> {

    @Override
    public Iterable<Tuple2<String, Long>> call(Tuple2<String, Iterable<String>> tuples) throws Exception {
      String word = tuples._1();
      Long count = 0L;
      for (String s : tuples._2()) {
        count++;
      }
      List<Tuple2<String, Long>> output = new ArrayList<>();
      output.add(new Tuple2<>(word, count));
      return output;
    }
  }
}
