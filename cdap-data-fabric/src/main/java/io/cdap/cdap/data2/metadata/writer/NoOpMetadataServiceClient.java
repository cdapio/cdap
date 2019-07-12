/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.writer;

import io.cdap.cdap.spi.metadata.MetadataMutation;

import java.util.List;

/**
 * NoOp Metadata service client that allows testing.
 */
public class NoOpMetadataServiceClient implements MetadataServiceClient {

  @Override
  public void create(MetadataMutation.Create createMutation) {
    // No Op
  }

  @Override
  public void drop(MetadataMutation.Drop dropMutation) {
    // No Op
  }

  @Override
  public void update(MetadataMutation.Update updateMutation) {
    // No Op
  }

  @Override
  public void remove(MetadataMutation.Remove removeMutation) {
    // No Op
  }

  @Override
  public void batch(List<MetadataMutation> mutations) {
    // No Op
  }
}
