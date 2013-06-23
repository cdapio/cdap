package com.continuuity.data.operation;

import javax.annotation.Nullable;
import java.util.Random;

/**
 * A read or write data operation.
 */
public abstract class Operation {

  // this is the same as in KeyValueTable!
  public static final byte [] KV_COL = { 'c' };

  // the unique id of the operation
  private final long id;

  // the name to use for metrics
  private String metricName;

  /**
   * @return unique Id associated with the operation.
   */
  public final long getId() {
    return this.id;
  }

  // used to generate random operation ids
  public static Random random = new Random();

  /**
   * Constructor without id - it is generated automatically.
   */
  protected Operation() {
    this.id = random.nextLong();
  }

  /**
   * Constructor with id - typically used for deserialization.
   */
  protected Operation(long id) {
    this.id = id;
  }

  /**
   * set a name to use for the data metrics - typically the name of
   * the data set that emitted the operation.
   * @param name the name to use
   */
  public void setMetricName(String name) {
    this.metricName = name;
  }

  /**
   * @return a name to use for the data metrics - ypically the name of
   *         the data set that emitted the operation
   */
  @Nullable
  public String getMetricName() {
    return this.metricName;
  }
}
