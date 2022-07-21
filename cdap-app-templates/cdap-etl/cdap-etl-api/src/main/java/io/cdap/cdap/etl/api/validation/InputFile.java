/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.etl.api.validation;

import java.io.IOException;

/**
 * Represents an input file that can be read
 */
public interface InputFile {

  /**
   * @return the name of the file
   */
  String getName();

  /**
   * @return length of the file in bytes
   */
  long getLength();

  /**
   * @return a SeekableInputStream for reading the file data
   * @throws IOException if there was an exception opening the file
   */
  SeekableInputStream open() throws IOException;
}
