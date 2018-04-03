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

package co.cask.cdap.api.lineage.field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a write operation from a collection of input fields into a destination of data.
 */
public class WriteOperation extends Operation {
  private final List<InputField> inputs;
  private final EndPoint destination;

  /***
   * Create an instance of write operation.
   *
   * @param name the name of the operation
   * @param description the description associated with the operation
   * @param destination the destination for the operation
   * @param inputs the array of inputs to be written
   */
  public WriteOperation(String name, String description, EndPoint destination, InputField... inputs) {
    this(name, description, destination, Arrays.asList(inputs));
  }

  /**
   * Create an instance of write operation.
   *
   * @param name the name of the operation
   * @param description the description associated with the operation
   * @param destination the destination for the operation
   * @param inputs the list of inputs to be written
   */
  public WriteOperation(String name, String description, EndPoint destination, List<InputField> inputs) {
    super(name, OperationType.WRITE, description);
    this.destination = destination;
    this.inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
  }

  /**
   * @return the destination where this operation writes
   */
  public EndPoint getDestination() {
    return destination;
  }

  /**
   * @return the list of input fields consumed by this write operation
   */
  public List<InputField> getInputs() {
    return inputs;
  }
}
