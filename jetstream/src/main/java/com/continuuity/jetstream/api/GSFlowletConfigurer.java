package com.continuuity.jetstream.api;

/**
 * Configures GSFlowlet.
 */
public interface GSFlowletConfigurer {

  void setName(String name);

  void setDescription(String description);

  void addGDATInput(String name, GSSchema schema);

  void addGSQL(String outputName, String gsql);
}
