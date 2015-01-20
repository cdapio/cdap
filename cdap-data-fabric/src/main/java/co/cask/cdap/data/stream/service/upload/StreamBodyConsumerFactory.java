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

import co.cask.http.BodyConsumer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * A factory for creating {@link BodyConsumer} to support the stream batch ingest endpoint.
 */
public class StreamBodyConsumerFactory {

  /**
   * Creates a {@link BodyConsumer} to handle the given batch request.
   *
   * @throws UnsupportedOperationException if no {@link BodyConsumer} can supports the given request content type
   */
  public BodyConsumer create(HttpRequest request,
                             ContentWriterFactory contentWriterFactory) throws UnsupportedOperationException {

    // Just hardcoded the supported type here, until we support a pluggable architecture
    String contentType = request.getHeader(HttpHeaders.Names.CONTENT_TYPE);
    if (contentType == null) {
      throw new UnsupportedOperationException("Unknown content type");
    }

    if (contentType.startsWith("text/")) {
      return new TextStreamBodyConsumer(contentWriterFactory);
    }
    throw new UnsupportedOperationException("Unsupported content type " + contentType);
  }
}
