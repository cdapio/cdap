/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.data.stream;

import javax.annotation.Nullable;

/**
 *  Streams are the primary means for pushing data from external systems
 *  into CDAP. Each individual event or signal sent to a Stream
 *  is stored as an Event, which is comprised of a body (blob of arbitrary
 *  binary data) and headers (map of strings for metadata).Within the system,
 *  Streams are identified by a Unique ID string and must be explicitly created
 *  before being used.
 */
public final class Stream {
  private final String name;
  private final String description;

  public Stream(String name) {
    this(name, null);
  }

  public Stream(String name, @Nullable String description) {
    this.name = name;
    this.description = description;
  }

 /**
  * Configures {@code Stream} by returning a {@link StreamSpecification}.
  *
  * @return Instance of {@link StreamSpecification}
  *
  */
  public StreamSpecification configure() {
    StreamSpecification.Builder builder = new StreamSpecification.Builder();
    builder.setName(name);
    if (description != null) {
      builder.setDescription(description);
    }
    return builder.create();
  }
}
