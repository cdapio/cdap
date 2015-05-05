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

package co.cask.cdap.cli.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 *
 */
public class FilePathResolverTest {

  @Test
  public void testResolve() {
    File homeDir = new File("/home/bob");
    File workingDir = new File("/working/dir");
    File cdapHomeDir = new File("/opt/cdap");

    FilePathResolver resolver = new FilePathResolver(homeDir, workingDir, cdapHomeDir);

    // test paths using ~/
    File file = resolver.resolvePathToFile("~/~t~est..1~123");
    File expectedFile = new File(homeDir, "~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("~/123/../~t~est..1~123");
    expectedFile = new File(homeDir, "~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    // test absolute paths

    file = resolver.resolvePathToFile("/../~t~est..1~123");
    expectedFile = new File("/~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("/../../~t~est..1~123");
    expectedFile = new File("/~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("/./~t~est..1~123");
    expectedFile = new File("/~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    // test relative paths

    file = resolver.resolvePathToFile("~t~est..1~123");
    expectedFile = new File(workingDir, "~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("123/../~t~est..1~123");
    expectedFile = new File(workingDir, "~t~est..1~123");
    Assert.assertEquals(expectedFile, file);

    // test $CDAP_HOME

    file = resolver.resolvePathToFile("$CDAP_HOME/sdf");
    expectedFile = new File(cdapHomeDir, "sdf");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("$CDAP_HOME-ff/sdf");
    expectedFile = new File(cdapHomeDir.getAbsolutePath() + "-ff", "sdf");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("$CDAP_HOME_/sdf");
    expectedFile = new File(workingDir, "$CDAP_HOME_/sdf");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("$CDAP_HOME9/sdf");
    expectedFile = new File(workingDir, "$CDAP_HOME9/sdf");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("$CDAP_HOMEf/sdf");
    expectedFile = new File(workingDir, "$CDAP_HOMEf/sdf");
    Assert.assertEquals(expectedFile, file);

    file = resolver.resolvePathToFile("$CDAP_HOMEF/sdf");
    expectedFile = new File(workingDir, "$CDAP_HOMEF/sdf");
    Assert.assertEquals(expectedFile, file);
  }
}
