/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.worker.AbstractWorker;

/**
 * App just emit metadata
 */
public class MetadataEmitApp extends AbstractApplication {
  public static final String NAME = "MetadataEmitApp";
  public static final Metadata METADATA = new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"),
                                                       ImmutableSet.of("tag1", "tag2", "tag3", "tag4"));

  @Override
  public void configure() {
    emitMetadata(new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2"), ImmutableSet.of("tag1", "tag2")));
    emitMetadata(new Metadata(ImmutableMap.of("k3", "v3", "k4", "v4"), ImmutableSet.of("tag3", "tag4")));
    addWorker(new NoopWorker());
  }

  public static class NoopWorker extends AbstractWorker {

    @Override
    protected void configure() {
      // no-op
    }

    @Override
    public void run() {
      // no-op
    }
  }
}
