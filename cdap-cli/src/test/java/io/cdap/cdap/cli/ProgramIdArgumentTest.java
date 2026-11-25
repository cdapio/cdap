/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ProgramIdArgument}.
 */
public class ProgramIdArgumentTest {

  @Test
  public void testToStringAndEquality() {
    ProgramIdArgument arg = new ProgramIdArgument("myApp", "myWorkflow");
    Assert.assertEquals("myApp", arg.getAppId());
    Assert.assertEquals("myWorkflow", arg.getProgramId());
    Assert.assertEquals("ProgramIdArgument{appId=myApp, programId=myWorkflow}", arg.toString());
    Assert.assertEquals(new ProgramIdArgument("myApp", "myWorkflow"), arg);
    Assert.assertEquals(new ProgramIdArgument("myApp", "myWorkflow").hashCode(), arg.hashCode());

    ProgramIdArgument nullArg = new ProgramIdArgument(null, null);
    Assert.assertEquals("ProgramIdArgument{appId=null, programId=null}", nullArg.toString());
  }
}
