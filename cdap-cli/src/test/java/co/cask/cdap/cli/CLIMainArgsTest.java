/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CLIMainArgsTest {

  @Test
  public void testParse() {
    String noArgFlag = "-" + CLIMain.HELP_OPTION.getOpt();
    String withArgFlag = "-" + CLIMain.AUTOCONNECT_OPTION.getOpt();
    Assert.assertEquals(true, CLIMain.AUTOCONNECT_OPTION.hasArg());

    Assert.assertEquals(
      new CLIMainArgs(new String[] {}, new String[] {"hello", "world"}),
      CLIMainArgs.parse(new String[]{"hello", "world"}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {noArgFlag}, new String[] {"hello", "world"}),
      CLIMainArgs.parse(new String[]{noArgFlag, "hello", "world"}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {}, new String[] {"hello", noArgFlag, "world"}),
      CLIMainArgs.parse(new String[]{"hello", noArgFlag, "world"}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {}, new String[] {"hello", "world", noArgFlag}),
      CLIMainArgs.parse(new String[]{"hello", "world", noArgFlag}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {withArgFlag, "somevalue"}, new String[] {"hello", "world"}),
      CLIMainArgs.parse(new String[]{withArgFlag, "somevalue", "hello", "world"}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {}, new String[] {"hello", withArgFlag, "world"}),
      CLIMainArgs.parse(new String[]{"hello", withArgFlag, "world"}, CLIMain.getOptions()));

    Assert.assertEquals(
      new CLIMainArgs(new String[] {}, new String[] {"hello", "world", withArgFlag}),
      CLIMainArgs.parse(new String[]{"hello", "world", withArgFlag}, CLIMain.getOptions()));
  }

}
