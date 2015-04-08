/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package org.apache.hadoop.yarn.util;

import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.batch.MapReduceContextConfig;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

/**
 * A {@link URLClassLoader} for YARN application isolation. Classes from
 * the application JARs are loaded in preference to the parent loader.
 */
public class ApplicationClassLoader extends ClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationClassLoader.class);

  private final List<ClassLoader> delegates;

  public ApplicationClassLoader() {
    this(createProgramClassLoader());
  }

  public ApplicationClassLoader(ClassLoader programClassLoader) {
    super(null);
    delegates = ImmutableList.of(programClassLoader, ApplicationClassLoader.class.getClassLoader());
  }

  @SuppressWarnings("unused")
  public ApplicationClassLoader(URL[] urls, ClassLoader parent, List<String> systemClasses) {
    this();
    // This constructor is retained for MR framework to call
  }

  @SuppressWarnings("unused")
  public ApplicationClassLoader(String classpath, ClassLoader parent,
                                List<String> systemClasses) throws MalformedURLException {
    this();
    // This constructor is retained for MR framework to call
  }

  public ClassLoader getProgramClassLoader() {
    return delegates.get(0);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    for (ClassLoader classLoader : delegates) {
      try {
        return classLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        LOG.trace("Class {} not found in ClassLoader {}", name, classLoader);
      }
    }

    throw new ClassNotFoundException("Class not found in all delegated ClassLoaders: " + name);
  }

  @Override
  protected URL findResource(String name) {
    for (ClassLoader classLoader : delegates) {
      URL url = classLoader.getResource(name);
      if (url != null) {
        return url;
      }
    }
    return null;
  }

  @Override
  protected Enumeration<URL> findResources(String name) throws IOException {
    Set<URL> urls = Sets.newHashSet();
    for (ClassLoader classLoader : delegates) {
      Iterators.addAll(urls, Iterators.forEnumeration(classLoader.getResources(name)));
    }
    return Iterators.asEnumeration(urls.iterator());
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    for (ClassLoader classLoader : delegates) {
      InputStream is = classLoader.getResourceAsStream(name);
      if (is != null) {
        return is;
      }
    }
    return null;
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
