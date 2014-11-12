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
 * the License
 */

package co.cask.cdap.cli.util;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Utility class to handle and format responses.
 */
public class ResponseUtil {
  private static final int DEFAULT_MAX_BODY_SIZE = 256;
  private static final int DEFAULT_LINE_WRAP_LIMIT = 64;
  private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");


  /**
   * Creates a string representing the body in the output. It only prints up to {@link #DEFAULT_MAX_BODY_SIZE},
   * with line wrap at each {@link #DEFAULT_LINE_WRAP_LIMIT} character.
   */
  public static String getBody(ByteBuffer body) {
    return getBody(body, DEFAULT_MAX_BODY_SIZE, DEFAULT_LINE_WRAP_LIMIT, DEFAULT_LINE_SEPARATOR);
  }

  /**
   * Creates a string representing the body in the output. It only prints up to {@code maxBodySize}, with line
   * wrap at each {@code lineWrapLimit} character.
   */
  public static String getBody(ByteBuffer body, int maxBodySize, int lineWrapLimit, String lineSeparator) {
    ByteBuffer bodySlice = body.slice();
    boolean hasMore = false;
    if (bodySlice.remaining() > maxBodySize) {
      bodySlice.limit(maxBodySize);
      hasMore = true;
    }

    String str = Bytes.toStringBinary(bodySlice) + (hasMore ? "..." : "");
    if (str.length() <= lineWrapLimit) {
      return str;
    }
    return Joiner.on(lineSeparator).join(Splitter.fixedLength(lineWrapLimit).split(str));
  }

  /**
   * Creates a string representing the output of response headers. Each key/value pair is outputted on its own
   * line in the form {@code <key> : <value>}.
   */
  public static String formatHeader(Map<String, String> headers) {
    StringBuilder builder = new StringBuilder();
    String separator = "";
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      builder.append(separator).append(entry.getKey()).append(" : ").append(entry.getValue());
      separator = DEFAULT_LINE_SEPARATOR;
    }
    return builder.toString();
  }
}
