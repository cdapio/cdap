/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.builder;

/**
 * Exception that is raised when there is an issue building the object.
 */
public class BuilderException extends RuntimeException {
  /**
   * Basic construction of exception.
   */
  public BuilderException() {
    super();
  }

  /**
   * Construction of exception with reason specified.
   * @param reason for why the exception was thrown.
   */
  public BuilderException(String reason) {
    super(reason);
  }

  /**
   * Construction of exception with a {@link Throwable}.
   * @param throwable instance.
   */
  public BuilderException(Throwable throwable) {
    super(throwable);
  }

  /**
   * Construction of exception with reason and throwable.
   * @param reason   for why the exception is being thrown.
   * @param throwable instance.
   */
  public BuilderException(String reason, Throwable throwable) {
    super(reason, throwable);
  }
}
