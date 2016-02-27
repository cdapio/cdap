/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.startup.check.NoOpCheck;
import co.cask.cdap.common.startup.check.fail.AlwaysFailCheck;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class CheckRunnerTest {
  private static final String FAILURE_MESSAGE = "test failure message";
  private static Injector injector;

  @BeforeClass
  public static void setupTests() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(AlwaysFailCheck.FAILURE_MESSAGE_KEY, FAILURE_MESSAGE);
    injector = Guice.createInjector(new ConfigModule(cConf));
  }

  @Test(expected = ClassNotFoundException.class)
  public void testNonexistentClassThrowsException() throws Exception {
    CheckRunner.builder(injector).addClass("asdf").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonCheckClassThrowsException() throws Exception {
    CheckRunner.builder(injector).addClass(getClass().getName());
  }

  @Test
  public void testMultipleChecks() throws Exception {
    CheckRunner checkRunner = CheckRunner.builder(injector)
      .addChecksInPackage(NoOpCheck.class.getPackage().getName())
      .build();

    List<CheckRunner.Failure> failures = checkRunner.runChecks();
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals(AlwaysFailCheck.NAME, failures.get(0).getName());
    Assert.assertEquals(FAILURE_MESSAGE, failures.get(0).getException().getMessage());
  }

  @Test
  public void testSingleCheck() throws Exception {
    CheckRunner checkRunner = CheckRunner.builder(injector)
      .addClass(NoOpCheck.class.getName())
      .build();

    List<CheckRunner.Failure> failures = checkRunner.runChecks();
    Assert.assertTrue(failures.isEmpty());
  }
}
