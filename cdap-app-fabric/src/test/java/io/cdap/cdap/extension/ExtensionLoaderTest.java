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

package io.cdap.cdap.extension;

import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.master.environment.MasterEnvironmentExtensionLoader;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link AbstractExtensionLoader}.
 */
public class ExtensionLoaderTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testLoadingFailure() throws IOException {
    ExtensionLoader loader = new ExtensionLoader(TMP_FOLDER.newFolder().getAbsolutePath());
    // getting all extensions should just skip the one that throws an error while loading
    Map<String, Extension> allExtensions = loader.getAll();
    Assert.assertEquals(2, allExtensions.size());
    Extension hiExtension = allExtensions.get("hi");
    Assert.assertEquals("hi", hiExtension.echo());
    Extension helloExtension = allExtensions.get("hello");
    Assert.assertEquals("hello", helloExtension.echo());
    // getting the extension that throws an error while loading should not propagate the exception
    Assert.assertNull(loader.get("error"));
  }

  @Test
  public void testMasterLoading() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set("app.program.runtime.extensions.dir",
        "/usr/local/google/home/ashau/dev/cdap/cdap-master/target/"
            + "stage-packaging/opt/cdap/master/ext/runtimes/");
    ProgramRuntimeProviderLoader loader = new ProgramRuntimeProviderLoader(cConf);
    ProgramRuntimeProvider provider = loader.get(ProgramType.SPARK);
    int x = 0;
    x++;
  }

  private static final class ExtensionLoader extends AbstractExtensionLoader<String, Extension> {

    public ExtensionLoader(String extDirs) {
      super(extDirs);
    }

    @Override
    protected Set<String> getSupportedTypesForProvider(Extension extension) {
      return extension.getSupported();
    }
  }

  /**
   * Test extension interface
   */
  public interface Extension {
    Set<String> getSupported();
    String echo();
  }

  /**
   * Extension that echoes 'hi'
   */
  @SuppressWarnings("unused")
  public static class HiExtension implements Extension {

    @Override
    public Set<String> getSupported() {
      return Collections.singleton("hi");
    }

    @Override
    public String echo() {
      return "hi";
    }
  }

  /**
   * Extension that echoes hello
   */
  @SuppressWarnings("unused")
  public static class HelloExtension implements Extension {

    @Override
    public Set<String> getSupported() {
      return Collections.singleton("hello");
    }

    @Override
    public String echo() {
      return "hello";
    }
  }

  /**
   * Extension that errors while loading
   */
  @SuppressWarnings("unused")
  public static class LoadingErrorExtension implements Extension {

    public LoadingErrorExtension() {
      throw new IllegalStateException("Failed to load");
    }

    @Override
    public Set<String> getSupported() {
      return Collections.singleton("error");
    }

    @Override
    public String echo() {
      return "error";
    }
  }
}
