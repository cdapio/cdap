/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.error;

/**
 * Centrally managed Err class where all the errors messages in the system are maintained.
 * <p>
 * Intent of centralizing is that they can be currated and managed in better way.
 * </p>
 */
public final class Err {
  /**
   * private constructor to prevent caller from creating Err object.
   */
  private Err() {

  }

  /**
   * Common Error messages that can be used in different contexts.
   */
  public static final Errors NOT_AN_ID = new Errors("'%s' name is not an ID. ID should be non empty and can contain" +
                                                      " only characters A-Za-z0-9_-");

  /**
   * Defines Schema related error messages.
   */
  public static class Schema {
    /**
     * Preventing construction.
     */
    private Schema() {
    }

    public static final Errors NOT_SUPPORTED_TYPE = new Errors(
      "Type %s is not supported. " +
        "Only Class or ParameterizedType are supported"
    );
  }

  /**
   * Defines Application level error messages.
   */
  public static class Application {
    /**
     * preventing construction
     */
    private Application() {
    }

    public static final Errors ATLEAST_ONE_PROCESSOR = new Errors(
      "Application %s has no program defined; " +
        "should have at least one program defined"
    );
  }

  /**
   * Defines Dataset specific error messages.
   */
  public static class DataSet {
    /**
     * Preventing Construction.
     */
    private DataSet() {
    }
  }

}
