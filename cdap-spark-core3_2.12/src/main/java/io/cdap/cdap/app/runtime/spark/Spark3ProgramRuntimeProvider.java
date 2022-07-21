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

package io.cdap.cdap.app.runtime.spark;

import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.runtime.spi.SparkCompat;

/**
 * Spark2 program runtime provider.
 */
@ProgramRuntimeProvider.SupportedProgramType(ProgramType.SPARK)
public class Spark3ProgramRuntimeProvider extends SparkProgramRuntimeProvider {

  public Spark3ProgramRuntimeProvider() {
    super(SparkCompat.SPARK3_2_12);
  }
}
