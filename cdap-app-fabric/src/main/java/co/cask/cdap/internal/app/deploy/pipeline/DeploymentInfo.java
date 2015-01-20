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

package co.cask.cdap.internal.app.deploy.pipeline;

import org.apache.twill.filesystem.Location;
import java.io.File;

/**
 * Contains information about input and output locations for deployment.
 */
public class DeploymentInfo {

  private final File input;
  private final Location destination;

  public DeploymentInfo(File input, Location destination) {
    this.input = input;
    this.destination = destination;
  }

  public File getInputLocation() {
    return input;
  }

  public Location getDestination() {
    return destination;
  }
}
