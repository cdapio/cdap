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

package io.cdap.cdap.graphql.store.application.typeresolver;

import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.application.schema.ApplicationTypes;
import io.cdap.cdap.graphql.typeresolver.CDAPTypeResolver;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;

/**
 * ProgramRecord type resolver. Registers the type resolver for the ProgramRecord interface
 */
public class ProgramRecordTypeResolver implements CDAPTypeResolver {

  private static final ProgramRecordTypeResolver INSTANCE = new ProgramRecordTypeResolver();

  private ProgramRecordTypeResolver() {

  }

  public static ProgramRecordTypeResolver getInstance() {
    return INSTANCE;
  }

  @Override
  public TypeRuntimeWiring getTypeResolver() {
    return TypeRuntimeWiring.newTypeWiring(ApplicationTypes.PROGRAM_RECORD)
      .typeResolver(
        typeResolutionEnvironment -> {
          ProgramRecord programRecord = typeResolutionEnvironment.getObject();

          if (programRecord.getType().equals(ProgramType.MAPREDUCE)) {
            return typeResolutionEnvironment.getSchema().getObjectType(ApplicationTypes.MAP_REDUCE);
          } else {
            return typeResolutionEnvironment.getSchema().getObjectType(ApplicationTypes.WORKFLOW);
          }
        }
      )
      .build();
  }
}
