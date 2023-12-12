/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ProgramType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Provider for runtime system of programs. */
public interface ProgramRuntimeProvider {

  /** Annotation for implementation to specify what are the supported {@link ProgramType}. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @interface SupportedProgramType {

    /** Returns the list of supported {@link ProgramType}. */
    ProgramType[] value();
  }

  /** The execution mode of the program runtime system. */
  enum Mode {
    LOCAL,
    DISTRIBUTED
  }

  /**
   * Creates a {@link ProgramRunner} for the given {@link ProgramType}.
   *
   * @param programType the {@link ProgramType} that the {@link ProgramRunner} will be used on
   * @param mode The execution mode that the {@link ProgramRunner} will be used one
   * @param injector the CDAP app-fabric Guice {@link Injector} for acquiring system services to
   *     interact with CDAP
   */
  ProgramRunner createProgramRunner(ProgramType programType, Mode mode, Injector injector);

  /**
   * Return whether the specified program type is supported. If not, {@link
   * #createProgramRunner(ProgramType, Mode, Injector)} will not be called.
   *
   * @param programType the {@link ProgramType} to check support for
   * @param cConf the CDAP configuration
   * @return whether the specified program type is supported
   */
  boolean isSupported(ProgramType programType, CConfiguration cConf);

  /**
   * Returns a ClassLoader for the given program type. This is useful if you only need the runtime
   * classloader for the given program type, but not for program execution.
   *
   * @param programType The type of program
   * @param cConf The configuration to use
   * @return a {@link ClassLoader} for the given program runner
   * @throws UnsupportedOperationException if the given program type is not supported by this
   *     provider. Caller can use the {@link #isSupported(ProgramType, CConfiguration)} method to
   *     check.
   */
  ClassLoader getRuntimeClassLoader(ProgramType programType, CConfiguration cConf);
}
