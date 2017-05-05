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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.proto.ProgramType;

/**
 * Spark2 program runtime provider.
 */
@ProgramRuntimeProvider.SupportedProgramType(ProgramType.SPARK)
public class Spark2ProgramRuntimeProvider extends SparkProgramRuntimeProvider {

  public Spark2ProgramRuntimeProvider() {
    super(SparkCompat.SPARK2_2_11);
  }
}
