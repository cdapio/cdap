/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.metric;

import com.continuuity.overlord.metrics.client.Info;
import com.continuuity.overlord.metrics.client.Type;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *   AbstractMetric
 */
public abstract class AbstractMetric implements Info {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetric.class);
  private final Info info;

  /**
   * Constructor
   * @param info Name/Description of Metric
   */
  protected AbstractMetric(Info info) {
    this.info = checkNotNull(info, "Info");
  }

  /**
   * @return Information about Metric.
   */
  protected Info getInfo() {
    return info;
  }

  /**
   * @return Value managed by Metric
   */
  public abstract Number getValue();

  /**
   * @return  {@link Type} of Metric.
   */
  public abstract Type getType();

  /**
   * @return Name of metric.
   */
  @Override
  public String getName() {
    return info.getName();
  }

  /**
   * @return Description of metric.
   */
  @Override
  public String getDescription() {
    return info.getDescription();
  }

  /**
   * @return String representation of Metric.
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                .add("info", info)
                .add("value", getValue())
                .toString();
  }

  /**
   * Compares two metric.
   * @param obj   other metric to be compared too
   * @return true if same, else false.
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof AbstractMetric) {
      final AbstractMetric other = (AbstractMetric) obj;
      return Objects.equal(info, other.getInfo()) &&
        Objects.equal(getValue(), other.getValue());
    }
    return false;
  }

  /**
   * @return hascode
   */
  @Override
   public int hashCode() {
      return Objects.hashCode(info, getValue());
    }
}
