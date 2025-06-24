/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.metadata.spanner;

import com.google.cloud.spanner.Mutation;
import io.cdap.cdap.spi.metadata.MetadataChange;
import java.util.List;

/**
 * A simple class to pass around a list of Spanner mutations, along with the
 * metadata change that it effects.
 */
public class ChangeRequest {

  private final List<Mutation> mutations;
  private final MetadataChange change;

  /**
   * Constructs a new Spanner change request.
   *
   * @param mutations the list of Spanner {@link Mutation}s to be applied.
   * @param change    the {@link MetadataChange} that describes the operation.
   */
  public ChangeRequest(List<Mutation> mutations, MetadataChange change) {
    this.mutations = mutations;
    this.change = change;
  }

  /**
   * Gets the list of Spanner mutations that represent the change to be applied.
   *
   * @return the list of Spanner {@link Mutation}s.
   */
  public List<Mutation> getMutation() {
    return mutations;
  }

  /**
   * Gets the object that describes the metadata change. This includes the
   * before and after state of the metadata.
   *
   * @return the {@link MetadataChange} that details the operation.
   */
  public MetadataChange getChange() {
    return change;
  }
}
