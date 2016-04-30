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

package co.cask.cdap.internal.app.runtime.artifact.plugin.endpointtest;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;

import javax.ws.rs.Path;

/**
 * plugin doesn't actually do anything, just for testing add artifact
 */
@Plugin(type = "valid")
@Name("extender")
@Description("This is a Plugin that uses TestEndpointPluginContext")
public class PluginEndpointContextTestPlugin {

  @Path("ping1")
  public String callMe(String input, TestEndpointPluginContext context) {
    return "hello";
  }

  // dupliate endpoint "ping"
  @Path("ping2")
  public String callMeAgain(String input, MockEndpointPluginContext context) {
    return "hello";
  }
}
