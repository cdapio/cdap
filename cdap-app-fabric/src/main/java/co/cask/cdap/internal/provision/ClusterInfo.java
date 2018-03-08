/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Information about a cluster for a program run.
 */
public class ClusterInfo {
  private final ClusterOp op;
  private final ProgramRunId programRunId;
  private final ProgramDescriptor programDescriptor;
  private final Map<String, String> provisionerProperties;
  private final String user;
  private final String provisionerName;
  private final Cluster cluster;

  public ClusterInfo(ProgramRunId programRunId, ProgramDescriptor programDescriptor,
                     Map<String, String> provisionerProperties, String provisionerName, String user,
                     ClusterOp op, @Nullable Cluster cluster) {
    this.programRunId = programRunId;
    this.provisionerProperties = provisionerProperties;
    this.programDescriptor = programDescriptor;
    this.user = user;
    this.provisionerName = provisionerName;
    this.op = op;
    this.cluster = cluster;
  }

  public ClusterInfo(ClusterInfo existing, ClusterOp op, @Nullable Cluster cluster) {
    this(existing.getProgramRunId(), existing.getProgramDescriptor(), existing.getProvisionerProperties(),
         existing.getProvisionerName(), existing.getUser(), op, cluster);
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public Map<String, String> getProvisionerProperties() {
    return provisionerProperties;
  }

  public String getUser() {
    return user;
  }

  public String getProvisionerName() {
    return provisionerName;
  }

  public ClusterOp getClusterOp() {
    return op;
  }

  @Nullable
  public Cluster getCluster() {
    return cluster;
  }

}
