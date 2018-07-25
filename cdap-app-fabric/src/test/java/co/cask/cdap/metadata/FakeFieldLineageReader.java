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

package co.cask.cdap.metadata;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.data2.metadata.lineage.field.EndPointField;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fake implementation of the {@link FieldLineageReader} for testing purpose.
 */
public class FakeFieldLineageReader implements FieldLineageReader {

  private final Set<String> fields;
  private final Set<EndPointField> summary;
  private final List<ProgramRunOperations> programRunOperations;

  public FakeFieldLineageReader(Set<String> fields, Set<EndPointField> summary,
                                Set<ProgramRunOperations> programRunOperations) {
    this.fields = Collections.unmodifiableSet(new HashSet<>(fields));
    this.summary = Collections.unmodifiableSet(new HashSet<>(summary));
    this.programRunOperations = Collections.unmodifiableList(new ArrayList<>(programRunOperations));
  }

  public FakeFieldLineageReader() {
    // create empty
    this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
  }

  @Override
  public Set<String> getFields(EndPoint endPoint, long start, long end) {
    return fields;
  }

  @Override
  public Set<EndPointField> getIncomingSummary(EndPointField endPointField, long start, long end) {
    return summary;
  }

  @Override
  public Set<EndPointField> getOutgoingSummary(EndPointField endPointField, long start, long end) {
    return summary;
  }

  @Override
  public List<ProgramRunOperations> getIncomingOperations(EndPointField endPointField, long start, long end) {
    return programRunOperations;
  }

  @Override
  public List<ProgramRunOperations> getOutgoingOperations(EndPointField endPointField, long start, long end) {
    return programRunOperations;
  }
}
