package com.continuuity.internal.app.services.legacy;

/**
 * Default implementation of MetaDefinition.
 */
public class MetaDefinitionImpl implements MetaDefinition {
  /**
   * Specifies the name of a flow. Has to be unique within a namespace.
   */
  private String name;

  /**
   * Email id to be associated with flow notifications.
   */
  private String email;

  /**
   * Name of the company that built the flow.
   */
  private String company;

  /**
   * Name of the app the flow is associated with.
   */
  private String app;

  /**
   * Default constructor.
   */
  public MetaDefinitionImpl() {

  }

  /**
   * Returns the name of the flow.
   *
   * @return name of the flow.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Sets name of a flow.
   *
   * @param name of a flow.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the email associated with the flow to which all failure status are reported to.
   *
   * @return email address associated with the flow.
   */
  @Override
  public String getEmail() {
    return email;
  }

  /**
   * Sets a new email id to be associated with a flow.
   *
   * @param email to be associated with flow.
   */
  public void setEmail(String email) {
    this.email = email;
  }

  /**
   * Returns name of the company who developed this flow.
   *
   * @return name of the company.
   */
  @Override
  public String getCompany() {
    return company;
  }

  /**
   * Sets the name of the company that built a flow.
   *
   * @param company that built the flow.
   */
  public void setCompany(String company) {
    this.company = company;
  }

  /**
   * Returns namespace associated with the flow.
   *
   * @return namespace of the flow.
   */
  @Override
  public String getApp() {
    return app;
  }

  /**
   * Sets the app name the flow is associated with.
   *
   * @param app name the flow is associated with.
   */
  public void setApp(String app) {
    this.app = app;
  }
}
