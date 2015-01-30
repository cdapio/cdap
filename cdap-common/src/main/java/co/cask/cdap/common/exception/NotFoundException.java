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

package co.cask.cdap.common.exception;

/**
 * Thrown when an element is not found
 */
public class NotFoundException extends Exception {

  private final String elementType;
  private final String elementId;

  public NotFoundException(String elementType, String elementId) {
    super(String.format("%s '%s' was not found", elementType, elementId));
    this.elementType = elementType;
    this.elementId = elementId;
  }

  /**
   * @return Type of the element: flow, stream, dataset, etc.
   */
  public String getElementType() {
    return elementType;
  }

  /**
   * @return ID of the element
   */
  public String getElementId() {
    return elementId;
  }
}
