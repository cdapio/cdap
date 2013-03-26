package com.continuuity.internal.api.batch.hadoop;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.hadoop.MapReduce;
import com.continuuity.api.batch.hadoop.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
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
  private final Map<String, String> arguments;
  private final String inputDataSet;
  private final String outputDataSet;

  public DefaultMapReduceSpecification(String name, String description, String inputDataSet, String outputDataSet, Set<String> dataSets, Map<String, String> arguments) {
    this(null, name, description, inputDataSet, outputDataSet, dataSets, arguments);
  }

  public DefaultMapReduceSpecification(MapReduce mapReduce) {
    this.className = mapReduce.getClass().getName();
    MapReduceSpecification configureSpec = mapReduce.configure();

    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.inputDataSet = configureSpec.getInputDataSet();
    this.outputDataSet = configureSpec.getOutputDataSet();
    this.dataSets = inspectDataSets(mapReduce.getClass(), ImmutableSet.<String>builder().addAll(configureSpec.getDataSets()));
    this.arguments = configureSpec.getArguments();
  }

  public DefaultMapReduceSpecification(String className, String name, String description, String inputDataSet, String outputDataSet, Set<String> dataSets, Map<String, String> arguments) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.inputDataSet = inputDataSet;
    this.outputDataSet = outputDataSet;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.arguments = arguments == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(arguments);
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
  public Map<String, String> getArguments() {
    return arguments;
  }

  @Override
  public String getOutputDataSet() {
    return outputDataSet;
  }

  @Override
  public String getInputDataSet() {
    return inputDataSet;
  }

  // TODO: duplicate code from DefaultProcedureSpec
  private Set<String> inspectDataSets(Class<?> classToInspect, ImmutableSet.Builder<String> datasets) {
    TypeToken<?> type = TypeToken.of(classToInspect);

    // Walk up the hierarchy of the class.
    for (TypeToken<?> typeToken : type.getTypes().classes()) {
      if (typeToken.getRawType().equals(Object.class)) {
        break;
      }

      // Grab all the DataSet
      for (Field field : typeToken.getRawType().getDeclaredFields()) {
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          datasets.add(dataset.value());
        }
      }
    }

    return datasets.build();
  }
}
