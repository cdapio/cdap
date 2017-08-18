/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.security;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Authorizable}
 */
public class AuthorizableTest {

  @Test
  public void testFromEntity() {
    ProgramId programId = new ProgramId("ns", "app", ProgramType.MAPREDUCE, "prog");
    Authorizable authorizable = Authorizable.fromEntityId(programId);
    Assert.assertEquals(programId.toString().replace(ApplicationId.DEFAULT_VERSION + ".", ""), authorizable.toString());
  }
}
