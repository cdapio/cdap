package com.continuuity.data.operation;

import com.continuuity.api.data.Operation;

/**
 * Administrative operation for formatting the data fabric with options for only
 * formatting user data, queues, and/or streams.
 */
public class FormatFabric implements Operation {

  private final boolean formatData;
  
  private final boolean formatQueues;
  
  private final boolean formatStreams;

  /**
   * Formats everything (user data, queues, and streams).
   */
  public FormatFabric() {
    this(true, true, true);
  }

  /**
   * Formats data, queues, and streams according to the specified boolean flags.
   * @param formatData true to format user data, false to not format queues
   * @param formatQueues true to format queues, false to not format queues
   * @param formatStreams true to format streams, false to not format streams
   */
  public FormatFabric(final boolean formatData, final boolean formatQueues,
      final boolean formatStreams) {
    this.formatData = formatData;
    this.formatQueues = formatQueues;
    this.formatStreams = formatStreams;
  }

  /**
   * Returns true if user data should be formatted.
   * @return true if user data should be formatted, false if not
   */
  public boolean shouldFormatData() {
    return this.formatData;
  }

  /**
   * Returns true if queues should be formatted.
   * @return true if queues should be formatted, false if not
   */
  public boolean shouldFormatQueues() {
    return this.formatQueues;
  }

  /**
   * Returns true if streams should be formatted.
   * @return true if streams should be formatted, false if not
   */
  public boolean shouldFormatStreams() {
    return this.formatStreams;
  }
}
