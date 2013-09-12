package com.continuuity.internal.batch;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.internal.DefaultResourceSpecification;
import com.continuuity.internal.ProgramSpecificationHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
  private final ResourceSpecification resources;

  public DefaultMapReduceSpecification(String name, String description, String inputDataSet, String outputDataSet,
                                       Set<String> dataSets, Map<String, String> arguments) {
    this(null, name, description, inputDataSet, outputDataSet, dataSets, arguments,
         DefaultResourceSpecification.create());
  }

  public DefaultMapReduceSpecification(MapReduce mapReduce) {
    this.className = mapReduce.getClass().getName();
    MapReduceSpecification configureSpec = mapReduce.configure();

    this.name = configureSpec.getName();
    this.description = configureSpec.getDescription();
    this.inputDataSet = configureSpec.getInputDataSet();
    this.outputDataSet = configureSpec.getOutputDataSet();
    this.dataSets = ProgramSpecificationHelper.inspectDataSets(
      mapReduce.getClass(), ImmutableSet.<String>builder().addAll(configureSpec.getDataSets()));
    this.arguments = configureSpec.getArguments();
    this.resources = DefaultResourceSpecification.create();
  }

  public DefaultMapReduceSpecification(String className, String name, String description, String inputDataSet,
                                       String outputDataSet, Set<String> dataSets, Map<String, String> arguments,
                                       ResourceSpecification resources) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.inputDataSet = inputDataSet;
    this.outputDataSet = outputDataSet;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.arguments = arguments == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(arguments);
    this.resources = resources;
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

  @Override
  public ResourceSpecification getResources() {
    return resources;
  }
}
