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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchSink;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.trace.TimestampedEvent;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Testing the Validation of etl stage hookups.
 */
public class PipelineTypeValidatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleInvalidMatch() throws Exception {
    // String --> Integer
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.add(Integer.class);
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAnotherInvalidMatch() throws Exception {
    // String --> Object | Object --> T | List<T> --> List<String>
    // List<Object> -> List<String> should fail.
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.add(Object.class);
    typeList.add(Object.class);
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(StringListSink.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testSimpleValidType() throws Exception {
    // Child --> Parent | Integer --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(TimestampedEvent.class);
    typeList.add(GenericRecord.class);
    typeList.add(Integer.class);
    typeList.add(Object.class);
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testParameterType() throws Exception {
    // String --> T | List<T> --> List<String>
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(StringListSink.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testRepeatStageType() throws Exception {
    // String --> T | List<T> --> T | List<T> --> T | List<T> --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(NoOpSink.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testGenericArrayType() throws Exception {
    // String[] --> E[] | List<E[]> --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String[].class);
    typeList.addAll(getBothParameters(ArrayToListArray.class));
    typeList.add(getFirstTypeParameter(NoOpSink.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testGenericParameterType() throws Exception {
    // String[] --> E[] | List<E[]> --> T | List<T> --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String[].class);
    typeList.addAll(getBothParameters(ArrayToListArray.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(NoOpSink.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  @Test
  public void testSourceParamType() throws Exception {
    // T --> N | List<N> --> M
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(getFirstTypeParameter(ParamToListParam.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(ParamToListParam.class));
    PipelineTypeValidator.validateTypes(typeList);
  }

  private static List<Type> getBothParameters(Class klass) {
    return Lists.newArrayList(getFirstTypeParameter(klass), getSecondTypeParameter(klass));
  }

  private static Type getTypeParameter(Class klass, int index) {
    return TypeToken.of(klass).resolveType(Transformation.class.getTypeParameters()[index]).getType();
  }

  private static Type getFirstTypeParameter(Class klass) {
    return getTypeParameter(klass, 0);
  }

  private static Type getSecondTypeParameter(Class klass) {
    return getTypeParameter(klass, 1);
  }

  private abstract static class ParamToListParam<T> extends Transform<T, List<T>> {

  }

  private abstract static class ArrayToListArray<E> extends Transform<E[], List<E[]>> {

  }

  private abstract static class StringListSink<A, B> extends BatchSink<List<String>, A, B> {

  }

  private abstract static class NoOpSink<X, Y> extends BatchSink<Object, X, Y> {

  }
}
