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

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.v1.ETLConfig;
import co.cask.cdap.etl.proto.v1.ETLStage;
import co.cask.cdap.etl.proto.v1.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.trace.TimestampedEvent;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Testing the Validation of etl stage hookups.
 */
public class PipelineRegistererTest {

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleInvalidMatch() throws Exception {
    // String --> Integer
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.add(Integer.class);
    PipelineRegisterer.validateTypes(typeList);
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
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test
  public void testSimpleValidType() throws Exception {
    // Child --> Parent | Integer --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(TimestampedEvent.class);
    typeList.add(GenericRecord.class);
    typeList.add(Integer.class);
    typeList.add(Object.class);
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test
  public void testParameterType() throws Exception {
    // String --> T | List<T> --> List<String>
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String.class);
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(StringListSink.class));
    PipelineRegisterer.validateTypes(typeList);
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
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test
  public void testGenericArrayType() throws Exception {
    // String[] --> E[] | List<E[]> --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String[].class);
    typeList.addAll(getBothParameters(ArrayToListArray.class));
    typeList.add(getFirstTypeParameter(NoOpSink.class));
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test
  public void testGenericParameterType() throws Exception {
    // String[] --> E[] | List<E[]> --> T | List<T> --> Object
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(String[].class);
    typeList.addAll(getBothParameters(ArrayToListArray.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(NoOpSink.class));
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test
  public void testSourceParamType() throws Exception {
    // T --> N | List<N> --> M
    ArrayList<Type> typeList = Lists.newArrayList();
    typeList.add(getFirstTypeParameter(ParamToListParam.class));
    typeList.addAll(getBothParameters(ParamToListParam.class));
    typeList.add(getFirstTypeParameter(ParamToListParam.class));
    PipelineRegisterer.validateTypes(typeList);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateStageName() throws Exception {
    ETLStage source = new ETLStage("DBSource", new Plugin("db", ImmutableMap.<String, String>of()));
    ETLStage transform1 = new ETLStage("TableValidation", new Plugin("validator", ImmutableMap.<String, String>of()));
    //duplicate transform name
    ETLStage transform2 = new ETLStage("TableValidation", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage sink = new ETLStage("AvroSink", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));
    PipelineRegisterer.validateStageNames(source,
                                          ImmutableList.of(transform1, transform2), ImmutableList.of(sink));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycleInConfig() throws Exception {
    ETLStage source = new ETLStage("DBSource", new Plugin("db", ImmutableMap.<String, String>of()));
    ETLStage transform1 = new ETLStage("TableValidation1", new Plugin("validator", ImmutableMap.<String, String>of()));
    //duplicate transform name
    ETLStage transform2 = new ETLStage("TableValidation2", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage transform3 = new ETLStage("TableValidation3", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage sink = new ETLStage("AvroSink", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));

    List<ETLStage> transforms = ImmutableList.of(transform1, transform2, transform3);
    List<Connection> connections = ImmutableList.of(new Connection("DBSource", "TableValidation1"),
                                                    new Connection("TableValidation1", "TableValidation2"),
                                                    new Connection("TableValidation1", "TableValidation3"),
                                                    // cycle connection
                                                    new Connection("TableValidation3", "TableValidation1"),
                                                    new Connection("TableValidation2", "AvroSink")
    );
    ETLConfig config = new ETLConfig(source, ImmutableList.of(sink), transforms, connections, new Resources());
    PipelineRegisterer.validateConnections(config);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testUnconnectedSinkConfig() throws Exception {
    ETLStage source = new ETLStage("DBSource", new Plugin("db", ImmutableMap.<String, String>of()));
    ETLStage transform1 = new ETLStage("TableValidation1", new Plugin("validator", ImmutableMap.<String, String>of()));
    //duplicate transform name
    ETLStage transform2 = new ETLStage("TableValidation2", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage transform3 = new ETLStage("TableValidation3", new Plugin("script", ImmutableMap.<String, String>of()));

    ETLStage sink1 = new ETLStage("AvroSink1", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));
    ETLStage sink2 = new ETLStage("AvroSink2", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));

    List<ETLStage> transforms = ImmutableList.of(transform1, transform2, transform3);
    List<ETLStage> sinks = ImmutableList.of(sink1, sink2);

    List<Connection> connections = ImmutableList.of(new Connection("DBSource", "TableValidation1"),
                                                    new Connection("TableValidation1", "TableValidation2"),
                                                    new Connection("TableValidation1", "TableValidation3"),
                                                    new Connection("TableValidation2", "AvroSink1"),
                                                    new Connection("TableValidation3", "AvroSink1")
                                                    // sink2 is unconnected
    );
    ETLConfig config = new ETLConfig(source, sinks, transforms, connections, new Resources());
    PipelineRegisterer.validateConnections(config);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testUnconnectedTransformConfig() throws Exception {
    ETLStage source = new ETLStage("DBSource", new Plugin("db", ImmutableMap.<String, String>of()));
    ETLStage transform1 = new ETLStage("TableValidation1", new Plugin("validator", ImmutableMap.<String, String>of()));
    //duplicate transform name
    ETLStage transform2 = new ETLStage("TableValidation2", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage transform3 = new ETLStage("TableValidation3", new Plugin("script", ImmutableMap.<String, String>of()));

    ETLStage sink1 = new ETLStage("AvroSink1", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));
    ETLStage sink2 = new ETLStage("AvroSink2", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));

    List<ETLStage> transforms = ImmutableList.of(transform1, transform2, transform3);
    List<ETLStage> sinks = ImmutableList.of(sink1, sink2);

    List<Connection> connections = ImmutableList.of(new Connection("DBSource", "TableValidation1"),
                                                    new Connection("TableValidation1", "AvroSink1"),
                                                    new Connection("TableValidation1", "AvroSink2"),
                                                    new Connection("TableValidation1", "TableValidation2"),
                                                    new Connection("TableValidation2", "TableValidation3")
                                                    // TablValidation3 is not connected to a sink
    );
    ETLConfig config = new ETLConfig(source, sinks, transforms, connections, new Resources());
    PipelineRegisterer.validateConnections(config);
  }

  @Test
  public void testTopologySorting() throws Exception {

    // linear
    Map<String, Set<String>> connectionsMap = new HashMap<>();
    connectionsMap.put("source", ImmutableSet.of("trA"));
    connectionsMap.put("trA", ImmutableSet.of("trB"));
    connectionsMap.put("trB", ImmutableSet.of("trC"));
    connectionsMap.put("trC", ImmutableSet.of("sink"));

    Assert.assertEquals(ImmutableList.of("source", "trA", "trB", "trC", "sink") ,
                        PipelineRegisterer.getStagesAfterTopologicalSorting(connectionsMap, "source"));


    connectionsMap.clear();
    connectionsMap.put("sourceA", ImmutableSet.of("transformB", "transformC"));
    connectionsMap.put("transformB", ImmutableSet.of("transformD"));
    connectionsMap.put("transformC", ImmutableSet.of("transformB", "transformD"));
    connectionsMap.put("transformD", ImmutableSet.of("sinkE"));

    Assert.assertEquals(ImmutableList.of("sourceA", "transformC", "transformB", "transformD", "sinkE") ,
                        PipelineRegisterer.getStagesAfterTopologicalSorting(connectionsMap, "sourceA"));


    connectionsMap.clear();
    connectionsMap.put("sourceA", ImmutableSet.of("transformB", "transformC", "transformD"));
    connectionsMap.put("transformB", ImmutableSet.of("transformD", "sink1"));
    connectionsMap.put("transformC", ImmutableSet.of("transformB", "transformE"));
    connectionsMap.put("transformD", ImmutableSet.of("sink1"));
    connectionsMap.put("transformE", ImmutableSet.of("transformB", "sink1"));


    Assert.assertEquals(ImmutableList.of("sourceA", "transformC", "transformE", "transformB", "transformD", "sink1") ,
                        PipelineRegisterer.getStagesAfterTopologicalSorting(connectionsMap, "sourceA"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCycleInTopologySorting() throws Exception {

    // linear
    Map<String, Set<String>> connectionsMap = new HashMap<>();
    connectionsMap.put("source", ImmutableSet.of("trA", "trB"));
    connectionsMap.put("trA", ImmutableSet.of("trB", "sink1"));
    connectionsMap.put("trB", ImmutableSet.of("trC"));
    connectionsMap.put("trC", ImmutableSet.of("trB", "sink2"));
    PipelineRegisterer.getStagesAfterTopologicalSorting(connectionsMap, "source");

  }


  @Test
  public void testValidSinkConfigs() throws Exception {
    ETLStage source = new ETLStage("DBSource", new Plugin("db", ImmutableMap.<String, String>of()));
    ETLStage transform1 = new ETLStage("TableValidation1", new Plugin("validator", ImmutableMap.<String, String>of()));
    //duplicate transform name
    ETLStage transform2 = new ETLStage("TableValidation2", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage transform3 = new ETLStage("TableValidation3", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage transform4 = new ETLStage("TableValidation4", new Plugin("script", ImmutableMap.<String, String>of()));
    ETLStage sink1 = new ETLStage("AvroSink1", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));
    ETLStage sink2 = new ETLStage("AvroSink2", new Plugin("tpfsAvro", ImmutableMap.<String, String>of()));

    List<ETLStage> transforms = ImmutableList.of(transform1, transform2, transform3, transform4);
    List<ETLStage> sinks = ImmutableList.of(sink1, sink2);

    List<Connection> connections = ImmutableList.of(new Connection("DBSource", "TableValidation1"),
                                                    new Connection("TableValidation1", "TableValidation2"),
                                                    new Connection("TableValidation1", "TableValidation3"),
                                                    new Connection("TableValidation2", "AvroSink1"),
                                                    new Connection("TableValidation3", "AvroSink1"),
                                                    new Connection("TableValidation3", "AvroSink2")
    );
    ETLConfig config = new ETLConfig(source, sinks, transforms, connections, new Resources());
    PipelineRegisterer.validateConnections(config);
    // test without any connections, the default should be used.
    config = new ETLConfig(source, sinks, transforms, new ArrayList<Connection>(), new Resources());
    PipelineRegisterer.validateConnections(config);

    // test with complex connections
    connections = ImmutableList.of(new Connection("DBSource", "TableValidation1"),
                                   new Connection("DBSource", "TableValidation2"),
                                   new Connection("TableValidation1", "TableValidation3"),
                                   new Connection("TableValidation2", "TableValidation1"),
                                   new Connection("TableValidation2", "TableValidation4"),
                                   new Connection("TableValidation4", "TableValidation3"),
                                   new Connection("TableValidation3", "AvroSink1"),
                                   new Connection("TableValidation4", "AvroSink1"),
                                   new Connection("TableValidation4", "AvroSink2")
    );
    config = new ETLConfig(source, sinks, transforms, connections, new Resources());
    PipelineRegisterer.validateConnections(config);

    // simple source to sink connections, no transforms and no connections
    config = new ETLConfig(source, sinks, new ArrayList<ETLStage>(), new ArrayList<Connection>(), new Resources());
    PipelineRegisterer.validateConnections(config);
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
