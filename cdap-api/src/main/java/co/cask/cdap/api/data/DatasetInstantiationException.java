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

package co.cask.cdap.api.data;

/**
 * This exception is thrown if - for whatever reason - a data set cannot be
 * instantiated at runtime.
 */
public class DatasetInstantiationException extends RuntimeException {

  public DatasetInstantiationException(String msg, Throwable e) {
    super(msg, e);
  }

  public DatasetInstantiationException(String msg) {
    super(msg);
  }
}
