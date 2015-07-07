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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.app.Application;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import javax.script.ScriptEngineFactory;

/**
 */
public class FilterClassLoaderTest {

  @Test(expected = ClassNotFoundException.class)
  public void testSystemInternalsHidden() throws ClassNotFoundException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    classLoader.loadClass(FilterClassLoader.class.getName());
  }

  @Test
  public void testAPIVisible() throws ClassNotFoundException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    Assert.assertSame(Application.class, classLoader.loadClass(Application.class.getName()));
  }

  @Test
  public void testBootstrapResourcesVisible() throws IOException {
    FilterClassLoader classLoader = FilterClassLoader.create(this.getClass().getClassLoader());
    Assert.assertNotNull(classLoader.getResource("META-INF/services/" + ScriptEngineFactory.class.getName()));
  }
}
