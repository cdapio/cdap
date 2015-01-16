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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.adapter.Sink;
import co.cask.cdap.adapter.Source;
import co.cask.cdap.proto.ProgramType;

import java.io.File;

/**
 * Holds information about an Adapter
 */
public final class AdapterInfo {
  //TODO: move somewhere more appropriate
  private final File file;
  private final String type;
  private final Source.Type sourceType;
  private final Sink.Type sinkType;
  private final String scheduleProgramId;
  private final ProgramType scheduleProgramType;

  public AdapterInfo(File file, String adapterType, Source.Type sourceType, Sink.Type sinkType,
                     String scheduleProgramId, ProgramType scheduleProgramType) {
    this.file = file;
    this.type = adapterType;
    this.sourceType = sourceType;
    this.sinkType = sinkType;
    this.scheduleProgramId = scheduleProgramId;
    this.scheduleProgramType = scheduleProgramType;
  }

  public File getFile() {
    return file;
  }

  public String getType() {
    return type;
  }

  public Source.Type getSourceType() {
    return sourceType;
  }

  public Sink.Type getSinkType() {
    return sinkType;
  }

  public String getScheduleProgramId() {
    return scheduleProgramId;
  }

  public ProgramType getScheduleProgramType() {
    return scheduleProgramType;
  }
}
