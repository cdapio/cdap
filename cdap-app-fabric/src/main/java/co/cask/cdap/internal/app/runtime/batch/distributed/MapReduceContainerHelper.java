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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * A helper class for dealing with MapReduce framework localization and classpath settings based on different
 * hadoop configurations
 */
public final class MapReduceContainerHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContainerHelper.class);

  /**
   * Adds the classpath to be used in MapReduce job execution based on the given {@link Configuration}.
   *
   * @param hConf the configuration for the job.
   * @param result a list for appending MR framework classpath
   * @return the same {@code result} list from the argument
   */
  public static List<String> addMapReduceClassPath(Configuration hConf, List<String> result) {
    String framework = hConf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH);

    // For classpath config get from the hConf, we splits it with both "," and ":" because one can set
    // the conf with something like "path1,path2:path3" and
    // it should become "path1:path2:path3" in the target JVM process
    Splitter splitter = Splitter.on(Pattern.compile(",|" + File.pathSeparatorChar)).trimResults().omitEmptyStrings();

    // If MR framework is non specified, use yarn.application.classpath and mapreduce.application.classpath
    // Otherwise, only use the mapreduce.application.classpath
    if (framework == null) {
      Iterable<String> yarnAppClassPath = Arrays.asList(
        hConf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
      Iterables.addAll(result, yarnAppClassPath);
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
  private static URI getFrameworkURI(Configuration hConf) {
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
      return ImmutableList.copyOf(addMapReduceClassPath(hConf, new ArrayList<String>()));
    } catch (URISyntaxException e) {
      // Shouldn't happen since the frameworkURI is already parsed.
      throw Throwables.propagate(e);
    }
  }

  private MapReduceContainerHelper() {
    // no-op
  }
}
