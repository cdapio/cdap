package com.continuuity.common.metrics;

import com.continuuity.common.builder.BuilderException;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Represents a client's request for adding a metricName to overlord.
 *
 * <p>
 *   In order to create a {@link MetricRequest} object you need to
 *   use the {@link MetricRequest.Builder} class.
 *
 *   Following is an example for creating a {@link MetricRequest}
 *   <pre>
 *     MetricRequest.Builder builder = new MetricRequest.Builder(true);
 *     builder.setRequestType(cmd);
 *     builder.setMetricName(name);
 *     builder.setMetricType(type);
 *     builder.setTimestamp(timestamp);
 *     builder.setValue(value);
 *     MetricRequest request = builder.create();
 *   </pre>
 * </p>
 */
public class MetricRequest {

  /**
   * Specifies the type of requests the metricName request server can
   * receive. For now we only have one PUT.
   */
  public enum MetricRequestType {
    PUT;
  };

  /**
   * Unique request ID.
   */
  private long requestId;

  /**
   * Defines the type of metricName request.
   */
  private MetricRequestType requestType;

  /**
   * Defines the type of metricName.
   */
  private MetricType metricType;

  /**
   * Defines the name of the metric.
   */
  private String metricName;

  /**
   * Defines the value associated with the metric.
   */
  private float value;

  /**
   * Defines the timestamp at which metric was measured.
   */
  private long timestamp;

  /**
   * Tags associated with the metric.
   */
  private List<ImmutablePair<String, String>> tags
    = Lists.newArrayList();

  /**
   * Specifies whether the metric request is a valid one or no.
   */
  private boolean valid;

  /**
   * Stores the raw request that was received.
   */
  private String rawRequest;

  /**
   * Constructor only exposed to the {@link Builder}.
   */
  private MetricRequest() {
    valid = true;
  }

  /**
   * Returns the type of the request made by the client.
   *
   * @return types defined in {@link MetricRequestType}
   */
  public MetricRequestType getRequestType() {
    return requestType;
  }

  private void setType(String requestType) {
    if ("put".equals(requestType)) {
      this.requestType = MetricRequestType.PUT;
    }
  }

  /**
   * Returns the type of the metric that was received.
   *
   * @return type of metric as specified by {@link MetricType}
   */
  public MetricType getMetricType() {
    return metricType;
  }

  private void setMetricType(MetricType metricType) {
    this.metricType = metricType;
  }

  /**
   * Returns whether the request object is a valid or no.
   *
   * @return true for valid; false otherwise.
   */
  public boolean getValid() {
    return valid;
  }

  private void setValid(boolean valid) {
    this.valid = valid;
  }

  /**
   * Returns the name of the metric.
   *
   * @return string representation of the metric.
   */
  public String getMetricName() {
    return metricName;
  }

  private void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * Returns the value measured by the metric.
   *
   * @return float instance of value for the metric.
   */
  public float getValue() {
    return value;
  }

  private void setValue(float value) {
    this.value = value;
  }

  /**
   * Returns the timestamp in the form of UNIX timestamp.
   *
   * @return timestamp at which the metric was measured.
   */
  public long getTimestamp() {
    return timestamp;
  }

  private void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Returns all the tags associated with the metric.
   *
   * @return list of tags associated with the metric.
   */
  public List<ImmutablePair<String, String>> getTags() {
    return tags;
  }

  private void setTags(List<ImmutablePair<String, String>> tags) {
    this.tags = tags;
  }

  /**
   * Returns the raw request that was parsed.
   *
   * @return raw request as sent to overlord.
   */
  public String getRawRequest() {
    return rawRequest;
  }

  private void setRawRequest(String rawRequest) {
    this.rawRequest = rawRequest;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("Request Type", requestType)
      .add("CMetric Type", metricType)
      .add("CMetric Name", metricName)
      .add("Timestamp", timestamp)
      .add("Value", value)
      .add("Tags", tags)
      .toString();
  }

  /**
   * Builder for creating the {@link MetricRequest} object.
   */
  public static class Builder {
    private String requestType;
    private MetricType metricType;
    private String metricName;
    private float value;
    private long timestamp;
    private List<ImmutablePair<String, String>> tags;
    private boolean valid;
    private String rawRequest;

    public Builder(boolean valid) {
      this.valid = valid;
      this.tags = Lists.newArrayList();
    }

    public Builder setRequestType(String requestType) {
      this.requestType = requestType;
      return this;
    }

    public Builder setMetricName(String metricName) {
      this.metricName = metricName;
      return this;
    }

    public Builder setMetricType(String type) {
      if (MetricType.FlowSystem.name().equals(type)) {
        metricType = MetricType.FlowSystem;
      } else if (MetricType.FlowUser.name().equals(type)) {
        metricType = MetricType.FlowUser;
      } else {
        metricType = MetricType.System;
      }
      return this;
    }

    public Builder setValue(float value) {
      this.value = value;
      return this;
    }

    public Builder setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder addTag(String name, String value) {
      tags.add(new ImmutablePair<String, String>(name, value));
      return this;
    }

    public Builder setRawRequest(String rawRequest) {
      rawRequest = rawRequest.replaceAll("(\\r|\\n)", "");
      this.rawRequest = rawRequest;
      return this;
    }

    /**
     * Creates an instance of MetricRequest.
     * <p>
     *   If the overall request is not valid, then no other fields
     *   in the request will be valid.
     * </p>
     *
     * @return an instance of MetricRequest.
     */
    public MetricRequest create() throws BuilderException {
      MetricRequest mpr = new MetricRequest();

      if (!valid) {
        mpr.setValid(valid);
        return mpr;
      }

      mpr.setRawRequest(rawRequest);
      mpr.setType(requestType);
      mpr.setMetricName(metricName);
      mpr.setMetricType(metricType);
      mpr.setValue(value);
      mpr.setTimestamp(timestamp);
      mpr.setTags(tags);
      return mpr;
    }
  }


}
