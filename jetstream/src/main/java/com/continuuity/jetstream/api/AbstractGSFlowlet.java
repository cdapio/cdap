package com.continuuity.jetstream.api;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Abstract class to implement JetStream Flowlet.
 */
public abstract class AbstractGSFlowlet extends AbstractFlowlet {
  private GSFlowletConfigurer configurer;

  /**
   * Override this method to configure the Jetstream Flowlet.
   */
  public abstract void create();

  protected void setName(String name) {
    configurer.setName(name);
  }

  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  protected void addGDATInput(String name, GSSchema schema) {
    configurer.addGDATInput(name, schema);
  }

  protected void addGSQL(String sqlName, String gsql, String sqlOutputName) {
    configurer.addGSQL(sqlName, gsql, sqlOutputName);
  }

}
