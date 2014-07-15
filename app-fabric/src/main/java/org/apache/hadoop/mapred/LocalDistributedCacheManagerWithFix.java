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

package org.apache.hadoop.mapred;


import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.ApiResourceListHolder;
import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.CombineClassLoader;
import com.continuuity.common.lang.jar.BundleJarUtil;
import com.continuuity.common.utils.DirUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A helper class for managing the distributed cache for {@link LocalJobRunner}.
 *
 * Continuuity fix is applied on the ClassLoader so that it doesn't keep opened file when the ClassLoader
 * is pending for GC.
 */
@SuppressWarnings("deprecation")
class LocalDistributedCacheManagerWithFix {
  public static final Log LOG =
    LogFactory.getLog(LocalDistributedCacheManagerWithFix.class);

  private List<String> localArchives = new ArrayList<String>();
  private List<String> localFiles = new ArrayList<String>();
  private List<String> localClasspaths = new ArrayList<String>();
  private List<File> jarExpandDirs = new ArrayList<File>();

  private List<File> symlinksCreated = new ArrayList<File>();

  private boolean setupCalled = false;

  /**
   * Set up the distributed cache by localizing the resources, and updating
   * the configuration with references to the localized resources.
   * @param conf
   * @throws IOException
   */
  public void setup(JobConf conf) throws IOException {

    String dir = String.format("%s/%s/%s", System.getProperty("user.dir"),
                                           conf.get(Constants.CFG_LOCAL_DATA_DIR),
                                           conf.get(Constants.AppFabric.OUTPUT_DIR));
    File workDir = new File(dir);

    // Generate YARN local resources objects corresponding to the distributed
    // cache configuration
    Map<String, LocalResource> localResources =
      new LinkedHashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
    // Generating unique numbers for FSDownload.
    AtomicLong uniqueNumberGenerator =
      new AtomicLong(System.currentTimeMillis());

    // Find which resources are to be put on the local classpath
    Map<String, Path> classpaths = new HashMap<String, Path>();
    Path[] archiveClassPaths = DistributedCache.getArchiveClassPaths(conf);
    if (archiveClassPaths != null) {
      for (Path p : archiveClassPaths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
                                                 remoteFS.getWorkingDirectory()));
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    Path[] fileClassPaths = DistributedCache.getFileClassPaths(conf);
    if (fileClassPaths != null) {
      for (Path p : fileClassPaths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
                                                 remoteFS.getWorkingDirectory()));
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }

    // Localize the resources
    LocalDirAllocator localDirAllocator =
      new LocalDirAllocator(MRConfig.LOCAL_DIR);
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    ExecutorService exec = null;
    try {
      ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("LocalDistributedCacheManagerWithFix Downloader #%d")
        .build();
      exec = Executors.newCachedThreadPool(tf);
      Path destPath = localDirAllocator.getLocalPathForWrite(".", conf);
      Map<LocalResource, Future<Path>> resourcesToPaths = Maps.newHashMap();
      for (LocalResource resource : localResources.values()) {
        Callable<Path> download =
          new FSDownload(localFSFileContext, ugi, conf,
                         new Path(destPath, Long.toString(uniqueNumberGenerator.incrementAndGet())),
                         resource);
        Future<Path> future = exec.submit(download);
        resourcesToPaths.put(resource, future);
      }
      for (Entry<String, LocalResource> entry : localResources.entrySet()) {
        LocalResource resource = entry.getValue();
        Path path;
        try {
          path = resourcesToPaths.get(resource).get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
        String pathString = path.toUri().toString();
        String link = entry.getKey();
        String target = new File(path.toUri()).getPath();
        symlink(workDir, target, link);

        if (resource.getType() == LocalResourceType.ARCHIVE) {
          localArchives.add(pathString);
        } else if (resource.getType() == LocalResourceType.FILE) {
          localFiles.add(pathString);
        } else if (resource.getType() == LocalResourceType.PATTERN) {
          //PATTERN is not currently used in local mode
          throw new IllegalArgumentException("Resource type PATTERN is not " +
                                               "implemented yet. " + resource.getResource());
        }
        Path resourcePath;
        try {
          resourcePath = ConverterUtils.getPathFromYarnURL(resource.getResource());
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
        LOG.info(String.format("Localized %s as %s", resourcePath, path));
        String cp = resourcePath.toUri().getPath();
        if (classpaths.keySet().contains(cp)) {
          localClasspaths.add(path.toUri().getPath().toString());
        }
      }
    } finally {
      if (exec != null) {
        exec.shutdown();
      }
    }
    // Update the configuration object with localized data.
    if (!localArchives.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
        .arrayToString(localArchives.toArray(new String[localArchives.size()])));
    }
    if (!localFiles.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALFILES, StringUtils
        .arrayToString(localFiles.toArray(new String[localArchives.size()])));
    }
    setupCalled = true;
  }

  /**
   * Utility method for creating a symlink and warning on errors.
   *
   * If link is null, does nothing.
   */
  private void symlink(File workDir, String target, String link)
    throws IOException {
    if (link != null) {
      link = workDir.toString() + Path.SEPARATOR + link;
      File flink = new File(link);
      if (!flink.exists()) {
        LOG.info(String.format("Creating symlink: %s <- %s", target, link));
        if (0 != FileUtil.symLink(target, link)) {
          LOG.warn(String.format("Failed to create symlink: %s <- %s", target,
                                 link));
        } else {
          symlinksCreated.add(new File(link));
        }
      }
    }
  }

  /**
   * Are the resources that should be added to the classpath? 
   * Should be calle after setup().
   *
   */
  public boolean hasLocalClasspaths() {
    if (!setupCalled) {
      throw new IllegalStateException(
        "hasLocalClasspaths() should be called after setup()");
    }
    return !localClasspaths.isEmpty();
  }

  /**
   * Creates a class loader that includes the designated
   * files and archives.
   *
   * Continnuuity fix : for each localClasspaths, if it is JAR file, uses the JarClassLoader instead
   * so that it won't keep the file stream opened, but rather having all classes bytes loaded in memmory.
   * If the class path is a directory, it will use URLClassLoader. The final ClassLoader is a CombineClassLoader
   * that load classes from all the JarClassLoader and the URLClassLoader as described above.
   */
  public ClassLoader makeClassLoader(final ClassLoader parent)
    throws MalformedURLException {

    final LocationFactory lf = new LocalLocationFactory();
    final List<ClassLoader> classLoaders = Lists.newArrayList();
    final List<URL> urls = Lists.newArrayList();
    for (final String classPath : localClasspaths) {
      final URI uri = new File(classPath).toURI();
      if (classPath.endsWith(".jar")) {
        classLoaders.add(AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
          @Override
          public ClassLoader run() {
            try {
              File expandDir = Files.createTempDir();
              jarExpandDirs.add(expandDir);
              return ClassLoaders.newProgramClassLoader(
                BundleJarUtil.unpackProgramJar(lf.create(uri), expandDir),
                ApiResourceListHolder.getResourceList(), parent);
            } catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }));
      } else {
        urls.add(uri.toURL());
      }
    }

    if (!urls.isEmpty()) {
      classLoaders.add(AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
        @Override
        public ClassLoader run() {
          return new URLClassLoader(urls.toArray(new URL[urls.size()]), parent);
        }
      }));
    }

    return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
      @Override
      public ClassLoader run() {
        return new CombineClassLoader(parent, classLoaders);
      }
    });
  }

  public void close() throws IOException {
    for (File symlink : symlinksCreated) {
      if (!symlink.delete()) {
        LOG.warn("Failed to delete symlink created by the local job runner: " +
                   symlink);
      }
    }
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    for (String archive : localArchives) {
      localFSFileContext.delete(new Path(archive), true);
    }
    for (String file : localFiles) {
      localFSFileContext.delete(new Path(file), true);
    }
    for (File dir : jarExpandDirs) {
      try {
        DirUtils.deleteDirectoryContents(dir);
      } catch (IOException e) {
        LOG.warn("Failed to delete jar directory " + dir);
      }
    }
  }
}
