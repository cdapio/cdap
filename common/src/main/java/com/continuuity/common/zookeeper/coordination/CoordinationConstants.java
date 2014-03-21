/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.discovery.Discoverable;

/**
 * Contains constants used by Resource Coordination service.
 */
final class CoordinationConstants {

  /**
   * The base zk folder for storing resource requirement.
   */
  static final String REQUIREMENTS_PATH = "/requirements";

  /**
   * The base zk folder for storing resource assignment.
   */
  static final String ASSIGNMENTS_PATH = "/assignments";

  /**
   * The GSON object for encode/decode various objects used by the resource coordination service.
   */
  static final Gson GSON = new GsonBuilder()
                              .registerTypeAdapter(ResourceAssignment.class, new ResourceAssignmentCodec())
                              .registerTypeAdapter(Discoverable.class, new DiscoverableCodec())
                              .create();
  // Just an arbitrary upper limit to avoid infinite retry on ZK operation failure.
  static final int MAX_ZK_FAILURE_RETRY = 10;

  private CoordinationConstants() {
  }
}
