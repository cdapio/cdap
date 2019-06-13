/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.programrecord.typeresolver;

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordTypes;
import io.cdap.cdap.graphql.typeresolver.CDAPTypeResolver;
import io.cdap.cdap.proto.ProtoTrigger;

/**
 * ProtoTrigger type resolver. Registers the type resolver for the ProtoTrigger interface
 */
public class ProtoTriggerTypeResolver implements CDAPTypeResolver {

  private static final ProtoTriggerTypeResolver INSTANCE = new ProtoTriggerTypeResolver();

  private ProtoTriggerTypeResolver() {

  }

  public static ProtoTriggerTypeResolver getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeResolver() {
    return TypeRuntimeWiring.newTypeWiring(ProgramRecordTypes.PROTO_TRIGGER)
      .typeResolver(
        typeResolutionEnvironment -> {
          ProtoTrigger protoTrigger = typeResolutionEnvironment.getObject();

          if (protoTrigger instanceof ProtoTrigger.TimeTrigger) {
            return typeResolutionEnvironment.getSchema().getObjectType(ProgramRecordTypes.TIME_TRIGGER);
          } else {
            throw new RuntimeException("Could not find a ProtoTrigger with type " + protoTrigger.getClass());
          }
        }
      )
      .build();
  }
}
