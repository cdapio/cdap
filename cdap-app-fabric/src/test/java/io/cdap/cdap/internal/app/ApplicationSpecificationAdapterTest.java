/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app;

import com.google.gson.stream.JsonReader;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link ApplicationSpecificationAdapter}.
 */
public class ApplicationSpecificationAdapterTest {

  @Test
  public void testGetProgramIds() throws IOException {
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());

    Set<ProgramId> expectedIds = new HashSet<>();
    for (ProgramType programType : ProgramType.values()) {
      appSpec.getProgramsByType(programType).stream()
        .map(name -> appId.program(io.cdap.cdap.proto.ProgramType.valueOf(programType.name()), name))
        .forEach(expectedIds::add);
    }

    String json = ApplicationSpecificationAdapter.create().toJson(appSpec);
    try (JsonReader reader = new JsonReader(new StringReader(json))) {
      Set<ProgramId> programIds = ApplicationSpecificationAdapter.getProgramIds(appId, reader);
      Assert.assertEquals(expectedIds, programIds);
    }
  }
}
