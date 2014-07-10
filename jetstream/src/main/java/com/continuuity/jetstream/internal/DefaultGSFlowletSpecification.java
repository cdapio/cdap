package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;

import java.util.Map;

/**
 * Default GSFlowlet Specification.
 */
public class DefaultGSFlowletSpecification implements GSFlowletSpecification {
  private String name;
  private String description;
  private Map<String, GSSchema> inputSchema;
  private Map<String, String> gsql;

  public DefaultGSFlowletSpecification(String name, String description, Map<String, GSSchema> inputSchema,
                                       Map<String, String> gsql) {
    this.name = name;
    this.description = description;
    this.inputSchema = inputSchema;
    this.gsql = gsql;
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
  public Map<String, GSSchema> getInputSchema() {
    return inputSchema;
  }

  @Override
  public Map<String, String> getGSQL() {
    return gsql;
  }
}
