package com.continuuity.gateway;

/**
 * Constants is a utility class that contains a set of universal constants
 * that are used throughout the Gateway project.
 */
public final class Constants {
  /**
   * The prefix for all continuity classes.
   */
  public static final String CONTINUUITY_PREFIX = "X-Continuuity-";

  /**
   * The prefix for all gateway properties.
   */
  public static final String GATEWAY_PREFIX = "gateway.";

  /**
   * Used by the external client to indicate what end point an event goes to.
   */
  public static final String HEADER_DESTINATION_STREAM
    = CONTINUUITY_PREFIX + "Destination";

  /**
   * Created by gateway to annotate each event with the name of the collector
   * through which it was ingested.
   */
  public static final String HEADER_FROM_COLLECTOR
    = CONTINUUITY_PREFIX + "FromCollector";


} // end of Constants class
