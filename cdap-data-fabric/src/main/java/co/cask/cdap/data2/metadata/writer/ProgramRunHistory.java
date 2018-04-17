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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.proto.ProgramRunStatus;

/**
 * Class for carrying program run history emitted from program stop.
 */
public class ProgramRunHistory {
  private final long stopTime;
  private final ProgramRunStatus status;

  public ProgramRunHistory(long stopTime, ProgramRunStatus status) {
    this.stopTime = stopTime;
    this.status = status;
  }

  public long getStopTime() {
    return stopTime;
  }

  public ProgramRunStatus getStatus() {
    return status;
  }
}
