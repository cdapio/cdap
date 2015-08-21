/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.common.zookeeper.coordination;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Codec;
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
