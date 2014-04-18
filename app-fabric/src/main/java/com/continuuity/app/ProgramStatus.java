package com.continuuity.app;

/**
 * Class containing a program status.
 */
public class ProgramStatus {
  private String applicationId;
  private String runnableId;
  private String status;

  public ProgramStatus(String applicationId, String runnableId, String status) {
    this.applicationId = applicationId;
    this.runnableId = runnableId;
    this.status = status;
  }

  public String getStatus() {
    return this.status;
  }
}
