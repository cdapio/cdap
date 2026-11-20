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

package io.cdap.cdap.client.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConnectionConfig}.
 */
public class ConnectionConfigTest {

  @Test
  public void testToStringAndEquality() {
    ConnectionConfig config = new ConnectionConfig("cdap.example.com", 11015, false, "v3");
    Assert.assertEquals(
        "ConnectionConfig{hostname=cdap.example.com, port=11015, sslEnabled=false}",
        config.toString());

    ConnectionConfig sslNullPort = new ConnectionConfig("secure.example.com", null, true);
    Assert.assertEquals(
        "ConnectionConfig{hostname=secure.example.com, port=null, sslEnabled=true}",
        sslNullPort.toString());

    Assert.assertEquals(new ConnectionConfig(config), config);
    Assert.assertEquals(new ConnectionConfig(config).hashCode(), config.hashCode());
  }
}
