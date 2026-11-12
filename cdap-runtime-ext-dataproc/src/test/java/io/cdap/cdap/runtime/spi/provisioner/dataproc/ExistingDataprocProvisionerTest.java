/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ExistingDataprocProvisioner}.
 */
public class ExistingDataprocProvisionerTest {

  @Test
  public void testValidatePropertiesRejectsTokenEndpoint() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.TOKEN_ENDPOINT_KEY, "http://example.com/token");
    props.put("accountKey", "auto-detect");

    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> new ExistingDataprocProvisioner().validateProperties(props));
    Assert.assertTrue(e.getMessage().contains("cannot be set in a compute profile"));
  }
}
