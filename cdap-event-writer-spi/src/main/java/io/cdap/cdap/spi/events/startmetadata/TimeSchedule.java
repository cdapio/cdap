package io.cdap.cdap.spi.events.startmetadata;

import java.util.Map;

public class TimeSchedule {
  private final String namespace;
  private final String application;
  private final String version;
  private final String scheduleName;
  private final String cronExpression;
  private final Map<String, String> properties;

  public TimeSchedule(String namespace, String application, String version, String scheduleName,
                      String cronExpression, Map<String, String> properties) {
    this.namespace = namespace;
    this.application = application;
    this.version = version;
    this.scheduleName = scheduleName;
    this.cronExpression = cronExpression;
    this.properties = properties;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public String getScheduleName() {
    return scheduleName;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return "TimeSchedule{" +
      "namespace='" + namespace + '\'' +
      ", application='" + application + '\'' +
      ", scheduleName='" + scheduleName + '\'' +
      ", cronExpression='" + cronExpression + '\'' +
      ", properties=" + properties +
      '}';
  }
}
