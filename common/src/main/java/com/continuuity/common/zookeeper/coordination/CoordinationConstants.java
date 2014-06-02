/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.Codec;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.discovery.Discoverable;

import java.io.IOException;

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

  // Just an arbitrary upper limit to avoid infinite retry on ZK operation failure.
  static final int MAX_ZK_FAILURE_RETRY = 10;

  /**
   * The GSON object for encode/decode various objects used by the resource coordination service.
   */
  private static final Gson GSON = new GsonBuilder()
                              .registerTypeAdapter(ResourceAssignment.class, new ResourceAssignmentTypeAdapter())
                              .registerTypeAdapter(Discoverable.class, new DiscoverableCodec())
                              .create();

  /**
   * Codec for {@link ResourceRequirement}.
   */
  static final Codec<ResourceRequirement> RESOURCE_REQUIREMENT_CODEC = new Codec<ResourceRequirement>() {
    @Override
    public byte[] encode(ResourceRequirement requirement) throws IOException {
      return Bytes.toBytes(CoordinationConstants.GSON.toJson(requirement));
    }

    @Override
    public ResourceRequirement decode(byte[] data) throws IOException {
      String json = Bytes.toString(data);
      return CoordinationConstants.GSON.fromJson(json, ResourceRequirement.class);
    }
  };

  /**
   * Codec for {@link ResourceAssignment}.
   */
  static final Codec<ResourceAssignment> RESOURCE_ASSIGNMENT_CODEC = new Codec<ResourceAssignment>() {
    @Override
    public byte[] encode(ResourceAssignment assignment) throws IOException {
      return Bytes.toBytes(GSON.toJson(assignment));
    }

    @Override
    public ResourceAssignment decode(byte[] data) throws IOException {
      return GSON.fromJson(Bytes.toString(data), ResourceAssignment.class);
    }
  };

  private CoordinationConstants() {
  }
}
