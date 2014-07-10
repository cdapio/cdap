package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.api.GSFlowletConfigurer;
import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Default GSFlowletConfigurer.
 */
public class DefaultGSFlowletConfigurer implements GSFlowletConfigurer {
  private String name;
  private String description;
  private Map<String, GSSchema> inputGDATStreams = Maps.newHashMap();
  private Map<String, String> sqlMap = Maps.newHashMap();

  public DefaultGSFlowletConfigurer(AbstractGSFlowlet flowlet) {
    this.name = flowlet.getClass().getSimpleName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null.");
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addGDATInput(String name, GSSchema schema) {
    Preconditions.checkArgument(name != null, "Input Name cannot be null.");
    Preconditions.checkState(!inputGDATStreams.containsKey(name), "Input Name already exists.");
    this.inputGDATStreams.put(name, schema);
  }

  @Override
  public void addGSQL(String outputName, String gsql) {
    Preconditions.checkArgument(outputName != null, "Output Name cannot be null.");
    Preconditions.checkState(!sqlMap.containsKey(outputName), "Output Name already exists.");
    this.sqlMap.put(outputName, gsql);
  }

  public GSFlowletSpecification createGSFlowletSpec() {
    return new DefaultGSFlowletSpecification(name, description, inputGDATStreams, sqlMap);
  }
}
