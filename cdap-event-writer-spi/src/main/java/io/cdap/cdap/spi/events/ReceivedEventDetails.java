package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;
import java.util.Map;

public class ReceivedEventDetails {
  private final String data;
  private final String ackId;
  @Nullable
  private final Map<String, String> attributes;

  private ReceivedEventDetails(String data, String ackId, @Nullable Map<String, String> attributes) {
    this.data = data;
    this.ackId = ackId;
    this.attributes = attributes;
  }

  public static Builder getBuilder(String data, String ackId) {
    return new Builder(data, ackId);
  }

  public String getData() {
    return data;
  }

  public String getAckId() {
    return ackId;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    String result = "Event data: " + data + ", ACK ID: " + ackId;

    if (attributes == null) {
      return result;
    }

    StringBuilder attributeBuilder = new StringBuilder();
    for (Map.Entry<String, String> e : attributes.entrySet()) {
      attributeBuilder.append(e.getKey() + ":" + e.getValue());
    }
    return result + "\n" + String.join("\n", attributeBuilder);
  }

  public static class Builder {
    private final String data;
    private final String ackId;
    private Map<String, String> attributes;

    Builder(String data, String ackId) {
      this.data = data;
      this.ackId = ackId;
    }

    public Builder withAttributes(Map<String, String> attributes) {
      this.attributes = attributes;
      return this;
    }

    public ReceivedEventDetails build() {
      return new ReceivedEventDetails(data, ackId, attributes);
    }
  }


}
