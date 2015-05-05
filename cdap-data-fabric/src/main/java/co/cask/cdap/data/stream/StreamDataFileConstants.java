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

package co.cask.cdap.data.stream;

/**
 * Class to hold constants used in Stream data file.
 */
public final class StreamDataFileConstants {

  // Package constants, these are internal to the file
  static final int MAGIC_HEADER_SIZE = 2;
  static final byte[] MAGIC_HEADER_V1 = {'E', '1'};
  static final byte[] MAGIC_HEADER_V2 = {'E', '2'};

  static final byte[] INDEX_MAGIC_HEADER_V1 = {'I', '1'};

  /**
   * Sets of constants related to accessing Stream data file properties.
   */
  public static final class Property {

    /**
     * Constants for property keys
     */
    public static final class Key {
      // Key for the data block schema
      public static final String SCHEMA = "stream.schema";

      // Key to indicate all events in the file is of the same timestamp
      public static final String UNI_TIMESTAMP = "stream.uni.timestamp";

      // Key prefix for properties that will be defaulted to all events' header
      public static final String EVENT_HEADER_PREFIX = "event.";
    }

    /**
     * Constants for property values
     */
    public static final class Value {
      // Special value for Key.UNI_TIMESTAMP to indicate using the file close time timestamp for all events
      public static final String CLOSE_TIMESTAMP = "close.timestamp";
    }
  }

  private StreamDataFileConstants() {
  }
}
