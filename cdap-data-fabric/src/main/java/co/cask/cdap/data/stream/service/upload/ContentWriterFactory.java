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

package co.cask.cdap.data.stream.service.upload;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.proto.Id;

import java.io.IOException;
import java.util.Map;

/**
 * Factory for creating {@link ContentWriter}.
 */
public interface ContentWriterFactory {

  /**
   * Returns the name of the stream that all {@link ContentWriter} created by this factory will write to.
   */
  Id.Stream getStream();

  /**
   * Creates a {@link ContentWriter} with the given set of event headers added to each {@link StreamEvent}
   * written through the {@link ContentWriter} returned.
   */
  ContentWriter create(Map<String, String> headers) throws IOException;
}
