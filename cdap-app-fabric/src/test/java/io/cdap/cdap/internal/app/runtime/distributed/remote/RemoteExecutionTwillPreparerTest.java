/*
 *  Copyright © 2023 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.common.io.Locations;
import java.io.IOException;
import java.net.URL;
import joptsimple.OptionSpec;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RemoteExecutionTwillPreparerTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();


  @Test
  public void testScalaExclusion() throws IOException {
    Location location = Locations.toLocation(TMP_FOLDER.newFolder());

    ApplicationBundler bundler = new ApplicationBundler(new ScalaRejector());
    bundler.createBundle(
        location,
        ApplicationMasterMain.class,
        TwillContainerMain.class,
        OptionSpec.class);


  }

  private static class ScalaRejector extends ClassAcceptor {

    @Override
    public boolean accept(String className, URL classUrl, URL classPathUrl) {
      return !className.startsWith("scala.");
    }
  }
}
