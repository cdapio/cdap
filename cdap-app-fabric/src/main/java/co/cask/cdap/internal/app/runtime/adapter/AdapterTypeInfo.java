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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Map;

/**
 * Holds information about an Adapter
 */
public final class AdapterTypeInfo {

  private final File file;
  private final String type;
  private final Source.Type sourceType;
  private final Sink.Type sinkType;
  private final Map<String, String> defaultSourceProperties;
  private final Map<String, String> defaultSinkProperties;
  private final Map<String, String> defaultAdapterProperties;
  private final ProgramType programType;

  public AdapterTypeInfo(File file, String adapterType, Source.Type sourceType, Sink.Type sinkType,
                         Map<String, String> defaultSourceProperties,
                         Map<String, String> defaultSinkProperties,
                         Map<String, String> defaultAdapterProperties,
                         ProgramType programType) {
    this.file = file;
    this.type = adapterType;
    this.sourceType = sourceType;
    this.sinkType = sinkType;
    this.defaultSourceProperties = ImmutableMap.copyOf(defaultSourceProperties);
    this.defaultSinkProperties = ImmutableMap.copyOf(defaultSinkProperties);
    this.defaultAdapterProperties = ImmutableMap.copyOf(defaultAdapterProperties);
    this.programType = programType;
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

  public Map<String, String> getDefaultSourceProperties() {
    return defaultSourceProperties;
  }

  public Map<String, String> getDefaultSinkProperties() {
    return defaultSinkProperties;
  }

  public Map<String, String> getDefaultAdapterProperties() {
    return defaultAdapterProperties;
  }

  public ProgramType getProgramType() {
    return programType;
  }
}
