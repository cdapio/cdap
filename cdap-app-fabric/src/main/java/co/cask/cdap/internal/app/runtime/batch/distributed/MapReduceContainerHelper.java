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

package co.cask.cdap.internal.app.runtime.batch.distributed;

import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.asm.Methods;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Paths;
import org.apache.twill.launcher.FindFreePort;
import org.apache.twill.launcher.TwillLauncher;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * A helper class for dealing with MapReduce framework localization and classpath settings based on different
 * hadoop configurations
 */
public final class MapReduceContainerHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContainerHelper.class);

  /**
   * Returns a list of path to be used for the MapReduce framework classpath.
   *
   * @param hConf the configuration for the job.
   * @param result a list for appending MR framework classpath
   * @return the same {@code result} list from the argument
   */
  public static List<String> getMapReduceClassPath(Configuration hConf, List<String> result) {
    String framework = hConf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH);

    // For classpath config get from the hConf, we splits it with both "," and ":" because one can set
    // the conf with something like "path1,path2:path3" and
    // it should become "path1:path2:path3" in the target JVM process
    Splitter splitter = Splitter.on(Pattern.compile(",|" + File.pathSeparatorChar)).trimResults().omitEmptyStrings();

    // If MR framework is non specified, use yarn.application.classpath and mapreduce.application.classpath
    // Otherwise, only use the mapreduce.application.classpath
    if (framework == null) {
      String yarnClassPath = hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                       Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
      Iterables.addAll(result, splitter.split(yarnClassPath));
    }

    // Add MR application classpath
    Iterables.addAll(result, splitter.split(hConf.get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                                                      MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH)));
    return result;
  }

  /**
   * Gets the MapReduce framework URI based on the {@code mapreduce.application.framework.path} setting.
   *
   * @param hConf the job configuration
   * @return the framework URI or {@code null} if not present or if the URI in the config is invalid.
   */
  @Nullable
  public static URI getFrameworkURI(Configuration hConf) {
    String framework = hConf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH);
    if (framework == null) {
      return null;
    }

    try {
      // Parse the path. It can contains '#' to represent the localized file name
      URI uri = new URI(framework);
      String linkName = uri.getFragment();

      // The following resolution logic is copied from JobSubmitter in MR.
      FileSystem fs = FileSystem.get(hConf);
      Path frameworkPath = fs.makeQualified(new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));
      FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), hConf);
      frameworkPath = fc.resolvePath(frameworkPath);
      uri = frameworkPath.toUri();

      // If doesn't have localized name (in the URI fragment), then use the last part of the URI path as name
      if (linkName == null) {
        linkName = uri.getPath();
        int idx = linkName.lastIndexOf('/');
        if (idx >= 0) {
          linkName = linkName.substring(idx + 1);
        }
      }
      return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, linkName);
    } catch (URISyntaxException e) {
      LOG.warn("Failed to parse {} as a URI. MapReduce framework path is not used. Check the setting for {}.",
               framework, MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, e);
    } catch (IOException e) {
      LOG.warn("Failed to resolve {} URI. MapReduce framework path is not used. Check the setting for {}.",
               framework, MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, e);
    }
    return null;
  }

  /**
   * Sets the resources that need to be localized to program runner twill container that used as MapReduce client.
   *
   * @param hConf The hadoop configuration
   * @param localizeResources the map to be updated with the localized resources.
   * @return a list of extra classpaths need to be set for the program runner container.
   */
  public static List<String> localizeFramework(Configuration hConf, Map<String, LocalizeResource> localizeResources) {
    try {
      URI frameworkURI = getFrameworkURI(hConf);

      // If MR Application framework is used, need to localize the framework file to Twill container
      if (frameworkURI != null) {
        URI uri = new URI(frameworkURI.getScheme(), frameworkURI.getAuthority(), frameworkURI.getPath(), null, null);
        localizeResources.put(frameworkURI.getFragment(), new LocalizeResource(uri, true));
      }
      return ImmutableList.copyOf(getMapReduceClassPath(hConf, new ArrayList<String>()));
    } catch (URISyntaxException e) {
      // Shouldn't happen since the frameworkURI is already parsed.
      throw Throwables.propagate(e);
    }
  }

  // TODO(CDAP-3119): Begin Hack for TWILL-144
  /**
   * Creates the launcher.jar for launch the main application. This is the central logic for the hack.
   * It inserts an extra line at the beginning of the TwillLauncher.main() method to create an extra
   * symlink for the mr-framework name. This is due to TWILL-144 that always append a ".tar.gz" suffix to the actual
   * localized name which get rename later. However the rename happens too late for us since we need the TwillLauncher
   * be able to scan the directories to pickup all necessary jar files
   * to create the ClassLoader for the twill container.
   */
  public static void saveLauncher(final Configuration hConf,
                                  File file, List<String> classPaths) throws URISyntaxException, IOException {
    final String launcherName = TwillLauncher.class.getName();
    final String portFinderName = FindFreePort.class.getName();
    final String symLinkerName = MapReduceContainerSymLinker.class.getName();

    // Create a jar file with the TwillLauncher
    // Also a little utility to find a free port, used for debugging.
    try (final JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(file))) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = MapReduceContainerHelper.class.getClassLoader();
      }

      Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          Preconditions.checkArgument(
            className.startsWith(launcherName) || className.equals(portFinderName) || className.equals(symLinkerName),
            "Launcher jar should not have dependencies: %s", className);

          try (InputStream is = classUrl.openStream()) {
            jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));

            if (className.equals(launcherName)) {
              rewriteLauncher(hConf, is, jarOut);
            } else {
              ByteStreams.copy(is, jarOut);
            }
            return true;
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      }, launcherName, portFinderName, symLinkerName);

      addClassPaths(Constants.CLASSPATH, classPaths, jarOut);

      String yarnAppClassPath = hConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                          Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

      addClassPaths(Constants.APPLICATION_CLASSPATH, Splitter.on(",").trimResults().split(yarnAppClassPath), jarOut);
    }
  }

  /**
   * Rewrites the TwillLauncher bytecode as described
   * in {@link #saveLauncher(Configuration, File, List)}.
   *
   * @param hConf the hadoop configuration
   * @param sourceByteCode the original bytecode of the TwillLauncher
   * @param output output stream for writing the modified bytecode.
   * @throws IOException
   */
  private static void rewriteLauncher(Configuration hConf,
                                      InputStream sourceByteCode, OutputStream output) throws IOException {
    URI frameworkURI = getFrameworkURI(hConf);
    if (frameworkURI == null) {
      ByteStreams.copy(sourceByteCode, output);
      return;
    }

    // It is localized as archive, and due to TWILL-144, a suffix is added. We need to reverse the effect of it
    // by creating an extra symlink as the first line in the TwillLauncher.main() method.
    String ext = Paths.getExtension(frameworkURI.getPath());
    if (ext.isEmpty()) {
      ByteStreams.copy(sourceByteCode, output);
      return;
    }

    final String sourceName = frameworkURI.getFragment();
    final String targetName = sourceName + "." + ext;

    ClassReader cr = new ClassReader(sourceByteCode);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
        if (!name.equals("main")) {
          return mv;
        }
        Type[] argTypes = Type.getArgumentTypes(desc);
        if (argTypes.length != 1) {
          return mv;
        }
        Type argType = argTypes[0];
        if (argType.getSort() != Type.ARRAY
          || !String.class.getName().equals(argType.getElementType().getClassName())) {
          return mv;
        }

        return new AdviceAdapter(Opcodes.ASM5, mv, access, name, desc) {
          @Override
          protected void onMethodEnter() {
            visitLdcInsn(sourceName);
            visitLdcInsn(targetName);
            invokeStatic(Type.getType(MapReduceContainerSymLinker.class),
                         Methods.getMethod(void.class, "symlink", String.class, String.class));
          }
        };
      }
    }, ClassReader.EXPAND_FRAMES);

    output.write(cw.toByteArray());
  }

  private static void addClassPaths(String classpathId,
                                    Iterable<String> classPaths, JarOutputStream jarOut) throws IOException {
    if (!Iterables.isEmpty(classPaths)) {
      jarOut.putNextEntry(new JarEntry(classpathId));
      jarOut.write(Joiner.on(':').join(classPaths).getBytes(Charsets.UTF_8));
    }
  }

  // End Hack for TWILL-144

  private MapReduceContainerHelper() {
    // no-op
  }
}
