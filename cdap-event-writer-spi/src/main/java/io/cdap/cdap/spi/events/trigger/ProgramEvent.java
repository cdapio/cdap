package io.cdap.cdap.spi.events.trigger;

public class ProgramEvent {

  private final String runID;
  private final String programName;
  private final String applicationName;
  private final String namespace;

  public ProgramEvent(String runID, String programName, String applicationName, String namespace) {
    this.runID = runID;
    this.programName = programName;
    this.applicationName = applicationName;
    this.namespace = namespace;
  }

  public String getRunID() {
    return runID;
  }

  public String getProgramName() {
    return programName;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getNamespace() {
    return namespace;
  }

  @Override
  public String toString() {
    return "ProgramEvent{" +
      "runID='" + runID + '\'' +
      ", programName='" + programName + '\'' +
      ", applicationName='" + applicationName + '\'' +
      ", namespace='" + namespace + '\'' +
      '}';
  }
}
