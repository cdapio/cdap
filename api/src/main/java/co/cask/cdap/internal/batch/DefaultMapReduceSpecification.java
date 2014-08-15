/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.batch;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.cdap.internal.specification.PropertyFieldExtractor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class DefaultMapReduceSpecification implements MapReduceSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Set<String> dataSets;
  private final Map<String, String> properties;
  private final String inputDataSet;
  private final String outputDataSet;
  private final int mapperMemoryMB;
  private final int reducerMemoryMB;

  public DefaultMapReduceSpecification(String name, String description, String inputDataSet, String outputDataSet,
                                       Set<String> dataSets, Map<String, String> properties, int mapperMemoryMB,
                                       int reducerMemoryMB) {
    this(null, name, description, inputDataSet, outputDataSet, dataSets, properties, mapperMemoryMB, reducerMemoryMB);
  }

  public DefaultMapReduceSpecification(MapReduce mapReduce) {
    MapReduceSpecification configureSpec = mapReduce.configure();

    Set<String> dataSets = Sets.newHashSet(configureSpec.getDataSets());
    Map<String, String> properties = Maps.newHashMap(configureSpec.getProperties());

    Reflections.visit(mapReduce, TypeToken.of(mapReduce.getClass()),
                      new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(dataSets));

    this.className = mapReduce.getClass().getName();
    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.inputDataSet = configureSpec.getInputDataSet();
    this.outputDataSet = configureSpec.getOutputDataSet();

    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.properties = ImmutableMap.copyOf(properties);

    this.mapperMemoryMB = configureSpec.getMapperMemoryMB();
    this.reducerMemoryMB = configureSpec.getReducerMemoryMB();
  }

  public DefaultMapReduceSpecification(String className, String name, String description, String inputDataSet,
                                       String outputDataSet, Set<String> dataSets, Map<String, String> properties,
                                       int mapperMemoryMB, int reducerMemoryMB) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.inputDataSet = inputDataSet;
    this.outputDataSet = outputDataSet;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.properties = properties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(properties);
    this.mapperMemoryMB = mapperMemoryMB;
    this.reducerMemoryMB = reducerMemoryMB;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Set<String> getDataSets() {
    return dataSets;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public String getOutputDataSet() {
    return outputDataSet;
  }

  @Override
  public String getInputDataSet() {
    return inputDataSet;
  }

  @Override
  public int getMapperMemoryMB() {
    return mapperMemoryMB;
  }

  @Override
  public int getReducerMemoryMB() {
    return reducerMemoryMB;
  }
}
