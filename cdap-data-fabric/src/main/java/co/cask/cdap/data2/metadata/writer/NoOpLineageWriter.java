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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;

import javax.annotation.Nullable;

/**
 * No-op {@link LineageWriter}.
 */
public class NoOpLineageWriter implements LineageWriter, FieldLineageWriter {
  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetInstance, AccessType accessType) {
    // no-op
  }

  @Override
  public void addAccess(ProgramRunId run, DatasetId datasetInstance, AccessType accessType,
                        @Nullable NamespacedEntityId component) {
    // no-op
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId stream, AccessType accessType) {
    // no-op
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId stream, AccessType accessType,
                        @Nullable NamespacedEntityId component) {
    // no-op
  }

  @Override
  public void write(ProgramRunId programRunId, FieldLineageInfo info) {
    // no-op
  }
}
