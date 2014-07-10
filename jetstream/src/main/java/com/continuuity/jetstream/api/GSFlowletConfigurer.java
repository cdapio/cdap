package com.continuuity.jetstream.api;

/**
 * Configures GSFlowlet.
 */
public interface GSFlowletConfigurer {

  /**
   * Sets the GSFlowlet Name.
   * @param name Name of the GSFlowlet.
   */
  void setName(String name);

  /**
   * Sets the description of the GSFlowet.
   * @param description Description of the GSFlowlet.
   */
  void setDescription(String description);

  /**
   * Adds a GDAT Input to the Flowlet.
   * @param name Name of the Input.
   * @param schema Attach a schema to the Input.
   */
  void addGDATInput(String name, GSSchema schema);

  /**
   * Adds a GSQL query to the Flowlet.
   * @param outputName Name of the GSQL Query (also the name of the output stream).
   * @param gsql GSQL query.
   */
  void addGSQL(String outputName, String gsql);
}
