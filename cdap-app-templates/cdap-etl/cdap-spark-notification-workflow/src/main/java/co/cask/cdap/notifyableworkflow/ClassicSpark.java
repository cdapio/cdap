/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.notifyableworkflow;

import co.cask.cdap.api.spark.AbstractSpark;

/**
 *
 */
public class ClassicSpark extends AbstractSpark {
  private final String mainClassName;

  public ClassicSpark(String mainClassName) {
    this.mainClassName = mainClassName;
  }

  @Override
  protected void configure() {
    try {
      setMainClass(Class.forName(mainClassName));
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(String.format("Class '%s' not found.", mainClassName), e);
    }
  }
}
