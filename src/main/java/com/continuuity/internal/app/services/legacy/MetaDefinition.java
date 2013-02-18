package com.continuuity.internal.app.services.legacy;

/**
 * MetaDefinition is a read-only interface for reading the attributes of the
 * meta section of a flow.
 */
public interface MetaDefinition {

  /**
   * Returns the name of the flow.
   *
   * @return name of the flow.
   */
  public String getName();

  /**
   * Returns the email associated with the flow to which all failure status
   * are reported to.
   *
   * @return email address associated with the flow.
   */
  public String getEmail();

  /**
   * Returns name of the company who developed this flow.
   *
   * @return name of the company.
   */
  public String getCompany();

  /**
   * Returns application name the flow belongs to.
   *
   * @return application the flow belongs to.
   */
  public String getApp();
}
