/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.common.authorization;

import co.cask.cdap.proto.ProgramType;
import co.cask.common.authorization.ObjectId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Various helper functions to construct {@link ObjectId}s.
 */
public class ObjectIds {

  public static final String NAMESPACE = "namespace";
  public static final String APPLICATION = "app";
  public static final String ADAPTER = "adapter";
  public static final String FLOW = "flow";
  public static final String MAPREDUCE = "mapreduce";
  public static final String PROCEDURE = "procedure";
  public static final String SERVICE = "service";
  public static final String SPARK = "spark";
  public static final String WORKFLOW = "workflow";

  public static final Map<ProgramType, String> PROGRAM_TYPE_MAP = ImmutableMap.<ProgramType, String>builder()
    .put(ProgramType.FLOW, "flow")
    .put(ProgramType.MAPREDUCE, "mapreduce")
    .put(ProgramType.PROCEDURE, "procedure")
    .put(ProgramType.SERVICE, "service")
    .put(ProgramType.SPARK, "spark")
    .put(ProgramType.WEBAPP, "webapp")
    .put(ProgramType.WORKFLOW, "workflow")
    .build();

  static {
    for (ProgramType programType : ProgramType.values()) {
      Preconditions.checkState(PROGRAM_TYPE_MAP.containsKey(programType));
    }
  }

  public static ObjectId namespace(String id) {
    return new ObjectId(NAMESPACE, id);
  }

  public static ObjectId application(String namespaceId, String id) {
    return new ObjectId(namespace(namespaceId), APPLICATION, id);
  }

  public static ObjectId program(String namespaceId, String appId, ProgramType programType, String programId) {
    return new ObjectId(application(namespaceId, appId), getTypeString(programType), programId);
  }

  private static String getTypeString(ProgramType programType) {
    Preconditions.checkArgument(PROGRAM_TYPE_MAP.containsKey(programType));
    return PROGRAM_TYPE_MAP.get(programType);
  }

  public static ObjectId adapter(String namespaceId, String id) {
    return new ObjectId(namespace(namespaceId), ADAPTER, id);
  }
}
