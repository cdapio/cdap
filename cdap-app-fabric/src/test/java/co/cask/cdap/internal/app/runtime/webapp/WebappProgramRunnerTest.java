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

package co.cask.cdap.internal.app.runtime.webapp;

import co.cask.cdap.common.utils.Networks;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.URL;
import java.util.Set;

/**
 * Tests WebappProgramRunner.
 */
public class WebappProgramRunnerTest {
  @Test
  public void testGetServingHostNames() throws Exception {
    Set<String> expected = ImmutableSet.of(Networks.normalizeWebappDiscoveryName("127.0.0.1:20000/geo"),
                                           Networks.normalizeWebappDiscoveryName("127.0.0.1:20000/netlens"),
                                           Networks.normalizeWebappDiscoveryName("127.0.0.1:20000"),
                                           Networks.normalizeWebappDiscoveryName("default/netlens"),
                                           Networks.normalizeWebappDiscoveryName("www.abc.com:80/geo"));

    URL jarResource = getClass().getResource("/CountRandomWebapp-localhost.jar");
    Assert.assertNotNull(jarResource);
    InputSupplier<InputStream> jarInputSupplier = Resources.newInputStreamSupplier(jarResource);

    Set<String> hostnames = WebappProgramRunner.getServingHostNames(jarInputSupplier);
    Assert.assertEquals(expected, hostnames);
  }


  @Test
  public void testGetNoServingHostNames() throws Exception {
    URL jarResource = getClass().getResource("/test_explode.jar");
    Assert.assertNotNull(jarResource);

    InputSupplier<InputStream> jarInputSupplier = Resources.newInputStreamSupplier(jarResource);
    Set<String> hostnames = WebappProgramRunner.getServingHostNames(jarInputSupplier);
    Assert.assertTrue(hostnames.isEmpty());
  }
}
