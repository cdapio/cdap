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

package co.cask.cdap.app.store;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;

/**
 * Class for carrying program run history emitted from program stop.
 */
public class ProgramHisotrySource {
  private final ProgramRunId id;
  private final long stopTime;
  private final ProgramRunStatus status;
  private final byte[] startSourceId;
  private final byte[] stopSourceId;

  private final ArtifactId artifactId;

  public ProgramHisotrySource(ProgramRunId id, long stopTime, ProgramRunStatus status,
                              byte[] startSourceId, byte[] stopSourceId, ArtifactId artifactId) {
    this.id = id;
    this.stopTime = stopTime;
    this.status = status;
    this.startSourceId = startSourceId;
    this.stopSourceId = stopSourceId;
    this.artifactId = artifactId;
  }

  public ProgramRunId getId() {
    return id;
  }

  public long getStopTime() {
    return stopTime;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }

  public ArtifactId getArtifactId() {
    return artifactId;
  }

  public byte[] getStartSourceId() {
    return startSourceId;
  }

  public byte[] getStopSourceId() {
    return stopSourceId;
  }
}
