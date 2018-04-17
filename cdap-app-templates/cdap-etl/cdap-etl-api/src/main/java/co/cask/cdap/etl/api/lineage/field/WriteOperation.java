/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.etl.api.lineage.field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a write operation from a collection of input fields into a data sink.
 */
public class WriteOperation extends Operation {
  private final List<String> inputFields;
  private final EndPoint sink;

  /***
   * Create an instance of write operation.
   *
   * @param name the name of the operation
   * @param description the description associated with the operation
   * @param sink the sink for the operation
   * @param inputFields the array of input fields to be written
   */
  public WriteOperation(String name, String description, EndPoint sink, String... inputFields) {
    this(name, description, sink, Arrays.asList(inputFields));
  }

  /**
   * Create an instance of write operation.
   *
   * @param name the name of the operation
   * @param description the description associated with the operation
   * @param sink the sink for the operation
   * @param inputFields the list of input fields to be written
   */
  public WriteOperation(String name, String description, EndPoint sink, List<String> inputFields) {
    super(name, OperationType.WRITE, description);
    this.sink = sink;
    this.inputFields = Collections.unmodifiableList(new ArrayList<>(inputFields));
  }

  /**
   * @return the sink where this operation writes
   */
  public EndPoint getSink() {
    return sink;
  }

  /**
   * @return the list of input fields consumed by this write operation
   */
  public List<String> getInputFields() {
    return inputFields;
  }
}
