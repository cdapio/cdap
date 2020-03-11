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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.cdap.cdap.common.utils.DirUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SimpleTest {

  public static void main(String[] args) throws Exception {

//    CredentialsProvider credentialsProvider = FixedCredentialsProvider
//      .create(GoogleCredentials.getApplicationDefault());
//    JobControllerClient jobControllerClient = JobControllerClient.create(
//      JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
//        .setEndpoint("us-west1-dataproc.googleapis.com:443").build());
    Storage gcsStorage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of("launcher-two", "hello/world/how/are/you.jar");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    //gcsStorage.create(blobInfo, Files.readAllBytes(Paths.get("/Users/vinishashah/Downloads/launcher.jar")));

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

  private static URL[] getClassPathURLs(File dir, Set<String> libDirs) {
    try {
      List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
      addJarURLs(dir, urls);
      urls.add(new File(dir, "classes").toURI().toURL());

      for (String libDir : libDirs) {
        addJarURLs(new File(dir, libDir), urls);
      }

      return urls.toArray(new URL[urls.size()]);
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    for (File file : DirUtils.listFiles(dir, "jar")) {
      result.add(file.toURI().toURL());
    }
  }
}
