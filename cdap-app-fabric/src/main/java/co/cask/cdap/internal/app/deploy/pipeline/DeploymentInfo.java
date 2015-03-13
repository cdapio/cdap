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

  private final File appJarFile;
  private final Location destination;
  private final ApplicationDeployScope applicationDeployScope;
  private final boolean suspendSchedules;

  /**
   * Construct the DeploymentInfo with appJarFile and destination.
   *
   * @param appJarFile Application jar file that should be deployed. The File is expected to be present in the local
   *                   file system.
   * @param destination Destination that represents {@link Location} of the jar
   * @param suspendSchedules {@code true} if schedule should be suspended upon application deployment, {@code false}
   *                         otherwise
   */
  public DeploymentInfo(File appJarFile, Location destination, boolean suspendSchedules) {
    this.appJarFile = appJarFile;
    this.destination = destination;
    this.applicationDeployScope = ApplicationDeployScope.USER;
    this.suspendSchedules = suspendSchedules;
  }

  /**
   * Construct the DeploymentInfo with appJarFile, destination, and applicationScope.
   *
   * @param appJarFile Application jar file that should be deployed. The File is expected to be present in the local
   *                   file system.
   * @param destination Destination that represents {@link Location} of the jar
   * @param applicationDeployScope Scope that the application is being deployed in
   * @param suspendSchedules {@code true} if schedule should be suspended upon application deployment, {@code false}
   *                         otherwise
   */
  public DeploymentInfo(File appJarFile, Location destination, ApplicationDeployScope applicationDeployScope,
                        boolean suspendSchedules) {
    this.appJarFile = appJarFile;
    this.destination = destination;
    this.applicationDeployScope = applicationDeployScope;
    this.suspendSchedules = suspendSchedules;
  }

  public File getAppJarFile() {
    return appJarFile;
  }

  public Location getDestination() {
    return destination;
  }

  public ApplicationDeployScope getApplicationDeployScope() {
    return applicationDeployScope;
  }

  public boolean isSuspendSchedules() {
    return suspendSchedules;
  }
}
