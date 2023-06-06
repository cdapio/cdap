package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;

public class ReceivedEvent implements Event<ReceivedEventDetails> {

  private final long publishTime;
  private final String version;
  private final ReceivedEventDetails eventDetails;
  @Nullable
  private final String projectName;

  public ReceivedEvent(long publishTime, String version,
                       @Nullable String projectName, ReceivedEventDetails eventDetails) {
    this.publishTime = publishTime;
    this.version = version;
    this.projectName = projectName;
    this.eventDetails = eventDetails;
  }

  @Override
  public EventType getType() {
    return EventType.RECEIVED_EVENT;
  }

  @Override
  public long getPublishTime() {
    return publishTime;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Nullable
  @Override
  public String getInstanceName() {
    return null;
  }

  @Nullable
  @Override
  public String getProjectName() {
    return projectName;
  }


  @Override
  public ReceivedEventDetails getEventDetails() {
    return eventDetails;
  }

  @Override
  public String toString() {
    return "ReceivedEvent{" +
        "publishTime=" + publishTime +
        ", version='" + version + '\'' +
        ", projectName='" + projectName + '\'' +
        ", eventDetails=" + eventDetails +
        '}';
  }
}
