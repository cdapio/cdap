/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.runtimejob;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Joiner;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.DefaultLocalFile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 *
 */
public class SimpleTest {

  public static void main(String[] args) throws Exception {

    CredentialsProvider credentialsProvider = FixedCredentialsProvider
      .create(GoogleCredentials.getApplicationDefault());
    JobControllerClient jobControllerClient = JobControllerClient.create(
      JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
        .setEndpoint("us-west1-dataproc.googleapis.com:443").build());
    Storage gcsStorage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of("launcher-two", "hello/world");
    BlobId blobId2 = BlobId.of("launcher-two", "hello/world2");
    BlobId blobId3 = BlobId.of("launcher-two", "hello/world3");

    Set<String> filters = new HashSet<>();
    filters.add("status.state = ACTIVE");
    filters.add("labels.hello=world");

    JobControllerClient.ListJobsPagedResponse listJobsPagedResponse =
      jobControllerClient.listJobs("vini-project-238000", "us-west1", Joiner.on(" AND ").join(filters));

    Iterator<Job> jobsItor = listJobsPagedResponse.iterateAll().iterator();

    while (jobsItor.hasNext()) {
      System.out.println("job: " + jobsItor.next().getJobUuid());
    }

    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
   // gcsStorage.create(blobInfo, Files.readAllBytes(Paths.get("/Users/vinishashah/Downloads/launcher.jar")));
    gcsStorage.delete(blobId);

//    File applicationFile = new File("/Users/vinishashah/Downloads/application.jar");
//    File dir = new File("/Users/vinishashah/Desktop/expanded.application.jar");
//    unJar(applicationFile, dir);
//    List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
//    System.out.println("Classpath: " + Arrays.toString(getClassPathURLs(dir, ImmutableSet.of("lib"))));

//    Path cachePath = Files.createTempDirectory(Paths.get("/tmp").toAbsolutePath(), "launcher.cache");
//    LocationCache locationCache = new BasicLocationCache(new LocalLocationFactory().create(cachePath.toUri()));
//
//    Location launcherJar = locationCache.get("launcher.jar", new LocationCache.Loader() {
//      @Override
//      public void load(String name, Location targetLocation) throws IOException {
//        try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
//          ClassLoader classLoader = getClass().getClassLoader();
//          Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
//            @Override
//            public boolean accept(String className, URL classUrl, URL classPathUrl) {
//              if (className.startsWith("io.cdap.") || className.startsWith("io/cdap") ||
//                className.startsWith("org.apache.twill") || className.startsWith("org/apache/twill")) {
//                try {
//                  jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
//                  try (InputStream is = classUrl.openStream()) {
//                    ByteStreams.copy(is, jarOut);
//                  }
//                } catch (IOException e) {
//                  throw new RuntimeException(e);
//                }
//                return true;
//              }
//              return false;
//            }
//          }, DataprocJobMain.class.getName());
//        }
//      }
//    });

   // LocalLocationFactory lf = new LocalLocationFactory(Files.createTempDirectory("local.location").toFile());

//    ApplicationBundler bundler = DatarpocJarUtils.createBundler();
//
//    LocalFile twillJar = DatarpocJarUtils.getBundleJar(bundler, lf, "twill.jar",
//                                                       ImmutableList.of(ApplicationMasterMain.class,
//                                                                        TwillContainerMain.class, OptionSpec.class));

    /*Location launcherJar = lf.create("launcher.jar");
    try (JarOutputStream jarOut = new JarOutputStream(launcherJar.getOutputStream())) {
      ClassLoader classLoader = DataprocJobMain.class.getClassLoader();
      URL[] urls = new URL[1];
      urls[0] = new URL("file:/Users/vinishashah/Documents/work/mywork/cdap/cdap-runtime-spi/target/cdap-runtime" +
                          "-spi-6.2" +
                          ".0-SNAPSHOT.jar");
      URLClassLoader urlClassLoader = new URLClassLoader(urls, classLoader);
      Dependencies.findClassDependencies(urlClassLoader, new ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (className.equals("io.cdap.cdap.runtime.runtimejob.DataprocJobMain") ||
            className.equals("io.cdap.cdap.runtime.runtimejob.DataprocRuntimeEnvironment") ||
            className.equals("io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment")) {
            try {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              try (InputStream is = classUrl.openStream()) {
                ByteStreams.copy(is, jarOut);
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return true;
          }
          return false;
        }
      }, DataprocJobMain.class.getName());
    }
*/
    System.out.println();

  }

  private static void unJar(ZipInputStream input, File targetDirectory) throws IOException {
    Path targetPath = targetDirectory.toPath();
    Files.createDirectories(targetPath);

    ZipEntry entry;
    while ((entry = input.getNextEntry()) != null) {
      Path output = targetPath.resolve(entry.getName());

      if (entry.isDirectory()) {
        Files.createDirectories(output);
      } else {
        Files.createDirectories(output.getParent());
        Files.copy(input, output);
      }
    }
  }

  public static File unJar(File jarFile, File destinationFolder) throws IOException {
    try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(jarFile)))) {
      unJar(zipIn, destinationFolder);
    }
    return destinationFolder;
  }

//  private static URL[] getClassPathURLs(File dir, Set<String> libDirs) {
//    try {
//      List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
//      addJarURLs(dir, urls);
//      urls.add(new File(dir, "classes").toURI().toURL());
//
//      for (String libDir : libDirs) {
//        addJarURLs(new File(dir, libDir), urls);
//      }
//
//      return urls.toArray(new URL[urls.size()]);
//    } catch (MalformedURLException e) {
//      throw Throwables.propagate(e);
//    }
//  }

//  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
//    for (File file : DirUtils.listFiles(dir, "jar")) {
//      result.add(file.toURI().toURL());
//    }
//  }

  public static LocalFile getBundleJar(ApplicationBundler bundler, LocationFactory locationFactory, String name,
                                       Iterable<Class<?>> classes) throws IOException {
    Location targetLocation = locationFactory.create(name);
    bundler.createBundle(targetLocation, classes);
    return createLocalFile(name, targetLocation, true);
  }

  private static LocalFile createLocalFile(String name, Location location, boolean archive) throws IOException {
    return new DefaultLocalFile(name, location.toURI(), location.lastModified(), location.length(), archive, null);
  }
}
