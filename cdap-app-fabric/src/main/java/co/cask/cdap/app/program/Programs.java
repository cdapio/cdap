/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.app.program;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoaderProvider;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Factory helper to create {@link Program}.
 */
public final class Programs {

  /**
   * Creates a {@link Program} that can be executed by the given {@link ProgramRunner}.
   *
   * @param cConf the CDAP configuration
   * @param programRunner the {@link ProgramRunner} for executing the program. If provided and if it implements
   *                      {@link ProgramClassLoaderProvider}, then the
   *                      {@link ClassLoader} created for the {@link Program} will be determined based on it.
   *                      Otherwise, the {@link ClassLoader} will only have visibility
   *                      to cdap-api and hadoop classes.
   * @param programDescriptor description of the program to create
   * @param programJarLocation the {@link Location} of the program jar file
   * @param unpackedDir a directory that the program jar file was unpacked to
   * @return a new {@link Program} instance.
   * @throws IOException If failed to create the program
   */
  public static Program create(CConfiguration cConf, @Nullable ProgramRunner programRunner,
                               ProgramDescriptor programDescriptor,
                               Location programJarLocation, File unpackedDir) throws IOException {
    ClassLoader programParentClassLoader;
    if (programRunner instanceof ProgramClassLoaderProvider) {
      programParentClassLoader = ((ProgramClassLoaderProvider) programRunner).createProgramClassLoaderParent();
    } else {
      programParentClassLoader = FilterClassLoader.create(Programs.class.getClassLoader());
    }

    return new DefaultProgram(programDescriptor, programJarLocation,
                              new ProgramClassLoader(cConf, unpackedDir, programParentClassLoader));
  }

  private Programs() {
  }
}
