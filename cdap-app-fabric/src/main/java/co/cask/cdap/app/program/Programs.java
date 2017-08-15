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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramClassLoaderProvider;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Preconditions;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import java.util.Set;
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
    ClassLoader parentClassLoader = null;
    if (programRunner instanceof ProgramClassLoaderProvider) {
      parentClassLoader = ((ProgramClassLoaderProvider) programRunner).createProgramClassLoaderParent();
    }

    if (parentClassLoader == null) {
      parentClassLoader = FilterClassLoader.create(Programs.class.getClassLoader());
    }

    return new DefaultProgram(programDescriptor, programJarLocation,
                              new ProgramClassLoader(cConf, unpackedDir, parentClassLoader));
  }

  /**
   * Creates a new {@link Program} using information from an existing program. The new program has the same
   * runtime dependencies and must be from the same application as the original program.
   *
   * @param cConf the CDAP configuration
   * @param originalProgram the original program
   * @param programId the new program id
   * @param programRunner the {@link ProgramRunner} for executing the new program. If provided and if it implements
   *                      {@link ProgramClassLoaderProvider}, then the
   *                      {@link ClassLoader} created for the {@link Program} will be determined based on it.
   *                      Otherwise, the {@link ClassLoader} will only have visibility
   *                      to cdap-api and hadoop classes.
   * @return a new {@link Program} instance for the given programId
   * @throws IOException If failed to create the program
   */
  public static Program create(CConfiguration cConf, Program originalProgram,
                               ProgramId programId, @Nullable ProgramRunner programRunner) throws IOException {
    ClassLoader classLoader = originalProgram.getClassLoader();
    // The classloader should be ProgramClassLoader
    Preconditions.checkArgument(classLoader instanceof ProgramClassLoader,
                                "Program %s doesn't use ProgramClassLoader", originalProgram);

    // The new program should be in the same namespace and app
    ProgramId originalId = originalProgram.getId();
    Preconditions.checkArgument(originalId.getNamespaceId().equals(programId.getNamespaceId()),
                                "Program %s is not in the same namespace as %s", programId, originalId);
    Preconditions.checkArgument(originalId.getParent().equals(programId.getParent()),
                                "Program %s is not in the same application as %s", programId, originalId);

    // Make sure the program is defined in the app
    ApplicationSpecification appSpec = originalProgram.getApplicationSpecification();
    ensureProgramInApplication(appSpec, programId);

    return Programs.create(cConf, programRunner, new ProgramDescriptor(programId, appSpec),
                           originalProgram.getJarLocation(), ((ProgramClassLoader) classLoader).getDir());
  }

  private static void ensureProgramInApplication(ApplicationSpecification appSpec, ProgramId programId) {
    Set<String> nameSet;

    switch (programId.getType()) {
      case FLOW:
        nameSet = appSpec.getFlows().keySet();
        break;
      case MAPREDUCE:
        nameSet = appSpec.getMapReduce().keySet();
        break;
      case WORKFLOW:
        nameSet = appSpec.getWorkflows().keySet();
        break;
      case SERVICE:
        nameSet = appSpec.getServices().keySet();
        break;
      case SPARK:
        nameSet = appSpec.getSpark().keySet();
        break;
      case WORKER:
        nameSet = appSpec.getWorkers().keySet();
        break;
      default:
        // This shouldn't happen
        throw new IllegalArgumentException("Unsupported program type: " + programId.getType());
    }

    Preconditions.checkArgument(nameSet.contains(programId.getProgram()),
                                "%s is missing in application %s", programId, appSpec.getName());
  }

  private Programs() {
  }
}
