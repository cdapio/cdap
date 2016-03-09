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

package co.cask.cdap.data2.metadata.lineage;

/**
 * Represents the type of Dataset access by a Program.
 */
public enum AccessType {
  READ ('r'),
  WRITE ('w'),
  READ_WRITE ('a'),
  UNKNOWN('u');

  private final char type;

  AccessType(char type) {
    this.type = type;
  }

  public char getType() {
    return type;
  }

  public static AccessType fromType(char type) {
    switch (type) {
      case 'r':
        return READ;
      case 'w' :
        return WRITE;
      case 'a' :
        return READ_WRITE;
      case 'u' :
        return UNKNOWN;
    }
    throw new IllegalArgumentException("Invalid access type " + type);
  }
}
