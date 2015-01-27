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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.conf.Configuration;
import co.cask.cdap.data.runtime.main.ExploreCustomClassLoaderTwillRunnable.CustomResourcesClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;

public class CustomResourcesClassLoaderTest {
  private static final String configFilename = "custom-config/test-resource.xml";
  private static final String customConfDir = "custom-resource-classloader";
  
  @Test
  public void testGetResource() throws Exception {
    URL regularUrl = getClass().getClassLoader().getResource(configFilename);
    Assert.assertEquals("regular-classloader", getValue(regularUrl));

    URL customUrl = getCustomClassLoader().getResource(configFilename);
    Assert.assertEquals("custom-classloader", getValue(customUrl));
  }

  @Test
  public void testGetResourceAsStream() throws Exception {
    InputStream regularStream = getClass().getClassLoader().getResourceAsStream(configFilename);
    Assert.assertEquals("regular-classloader", getValue(regularStream));

    InputStream customStream = getCustomClassLoader().getResourceAsStream(configFilename);
    Assert.assertEquals("custom-classloader", getValue(customStream));
  }
  
  @Test
  public void testGetResources() throws Exception {
    URL regularUrl = getClass().getClassLoader().getResource(configFilename);
    Assert.assertNotNull(regularUrl);
    URL customUrl = getCustomClassLoader().getResource(configFilename);
    Assert.assertNotNull(customUrl);

    Assert.assertEquals(ImmutableList.of(regularUrl), 
                        enumerationToList(getClass().getClassLoader().getResources(configFilename)));
    Assert.assertEquals(ImmutableList.of(customUrl),
                        enumerationToList(getCustomClassLoader().getResources(configFilename)));
  }
  
  private CustomResourcesClassLoader getCustomClassLoader() throws MalformedURLException {
    return new CustomResourcesClassLoader(new URL[] {createCustomConfDirUrl()}, getClass().getClassLoader());
  }
  
  private <T> List<T> enumerationToList(Enumeration<T> enumeration) {
    List<T> list = Lists.newArrayList();
    while (enumeration.hasMoreElements()) {
      list.add(enumeration.nextElement());
    }
    return list;
  }

  private String getValue(URL url) {
    Configuration conf = new Configuration();
    conf.addResource(url);
    return conf.get("test");
  }

  private String getValue(InputStream in) {
    Configuration conf = new Configuration();
    conf.addResource(in);
    return conf.get("test");
  }

  private URL createCustomConfDirUrl() throws MalformedURLException {
    URL regularUrl = getClass().getClassLoader().getResource(configFilename);
    return new File(getParent(getParent(regularUrl)).getFile(), customConfDir).toURI().toURL();
  }
  
  private URL getParent(URL url) throws MalformedURLException {
    File file = new File(url.getFile());
    return new File(file.getParent()).toURI().toURL();
  }
}
