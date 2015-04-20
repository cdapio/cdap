/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A {@link ClassLoader} for YARN application isolation. Classes from
 * the application JARs are loaded in preference to the parent loader.
 */
public class MapReduceClassLoader extends CombineClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceClassLoader.class);

  public MapReduceClassLoader() {
    this(createProgramClassLoader());
  }

  public MapReduceClassLoader(ClassLoader programClassLoader) {
    super(null, ImmutableList.of(programClassLoader, MapReduceClassLoader.class.getClassLoader()));
  }

  public ClassLoader getProgramClassLoader() {
    return getDelegates().get(0);
  }

  /**
   * Creates a program {@link ClassLoader} based on the MR job config.
   */
  private static ClassLoader createProgramClassLoader() {
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    MapReduceContextConfig contextConfig = new MapReduceContextConfig(conf);

    // In distributed mode, the program is created by expanding the program jar.
    // The program jar is localized to container with the program jar name.
    // It's ok to expand to a temp dir in local directory, as the YARN container will be gone.
    Location programLocation = new LocalLocationFactory()
      .create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
    try {
      File unpackDir = DirUtils.createTempDir(new File(System.getProperty("user.dir")));
      LOG.info("Create ProgramClassLoader from {}, expand to {}", programLocation.toURI(), unpackDir);

      BundleJarUtil.unpackProgramJar(programLocation, unpackDir);
      return ProgramClassLoader.create(unpackDir, conf.getClassLoader(), ProgramType.MAPREDUCE);
    } catch (IOException e) {
      LOG.error("Failed to create ProgramClassLoader", e);
      throw Throwables.propagate(e);
    }
  }
}
