/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.LocalFileDescription;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import org.apache.twill.api.LocalFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default implementation of {@link RuntimeJobInfo}.
 */
public class DefaultRuntimeJobInfo implements RuntimeJobInfo {

  private final ProgramRunInfo info;
  private final Collection<LocalFileDescription> files;
  private final Map<String, String> jvmProperties;


  public DefaultRuntimeJobInfo(ProgramRunId programRunId, Collection<LocalFileDescription> files,
                               Map<String, String> jvmProperties) {
    this.info = new ProgramRunInfo.Builder()
      .setNamespace(programRunId.getNamespace())
      .setApplication(programRunId.getApplication())
      .setVersion(programRunId.getVersion())
      .setProgramType(programRunId.getType().name())
      .setProgram(programRunId.getProgram())
      .setRun(programRunId.getRun()).build();
    this.files = Collections.unmodifiableCollection(new ArrayList<>(files));
    this.jvmProperties = Collections.unmodifiableMap(new LinkedHashMap<>(jvmProperties));
  }

  @Override
  public ProgramRunInfo getProgramRunInfo() {
    return info;
  }

  @Override
  public Collection<LocalFileDescription> getLocalizeFiles() {
    return files;
  }

  @Override
  public String getRuntimeJobClassname() {
    return DefaultRuntimeJob.class.getName();
  }

  @Override
  public Map<String, String> getJvmProperties() {
    return jvmProperties;
  }
}
