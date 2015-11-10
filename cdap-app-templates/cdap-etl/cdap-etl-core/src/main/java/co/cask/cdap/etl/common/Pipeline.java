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

package co.cask.cdap.etl.common;

import java.util.List;

/**
 * Keeps track of the plugin ids for the source, transforms, and sink of a pipeline
 */
public class Pipeline {
  private final String source;
  private final List<SinkInfo> sinks;
  private final List<TransformInfo> transforms;

  public Pipeline(String source, List<SinkInfo> sinks, List<TransformInfo> transforms) {
    this.source = source;
    this.sinks = sinks;
    this.transforms = transforms;
  }

  public String getSource() {
    return source;
  }

  public List<SinkInfo> getSinks() {
    return sinks;
  }

  public List<TransformInfo> getTransforms() {
    return transforms;
  }
}
