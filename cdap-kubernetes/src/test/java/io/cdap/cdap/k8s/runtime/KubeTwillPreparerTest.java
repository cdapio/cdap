/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.DefaultResourceSpecification;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

/**
 * Tests for {@link KubeTwillPreparer}.
 */
public class KubeTwillPreparerTest {

  private ResourceSpecification createResourceSpecification() {
    return new DefaultResourceSpecification(1, 100,
                                            1, 1, 1);
  }

  @Test
  public void test() throws Exception {
    ApiClient apiClient = Config.defaultClient();
    System.out.println(apiClient.getBasePath());
  }

  @Test
  public void testWithDependentRunnables() throws Exception {
    TwillSpecification twillSpecification = TwillSpecification.Builder.with()
      .setName("NAME")
      .withRunnable()
      .add(new MainRunnable(), createResourceSpecification())
      .withLocalFiles()
      .add("cConf.xml", new URI(""))
      .apply()
      .add(new SidecarRunnable(),
           createResourceSpecification())
      .withLocalFiles()
      .add("cConf.xml", new URI(""))
      .apply()
      .add(new SidecarRunnable2())
      .withLocalFiles()
      .add("cConf.xml", new URI(""))
      .apply()
      .anyOrder()
      .build();

    KubeTwillPreparer preparer = new KubeTwillPreparer(null, null, "default",
                                                       null, twillSpecification, null, null,
                                                       null, null, null);

    // test catching main runnable depends on itself
    try {
      preparer.dependentRunnableNames(MainRunnable.class.getSimpleName(), MainRunnable.class.getSimpleName());
      Assert.fail("Expected IllegalArgumentException exception but got no exception");
    } catch (Exception ex) {
      Assert.assertThat(ex, CoreMatchers.instanceOf(IllegalArgumentException.class));
    }

    // test catching empty dependent runnables
    try {
      preparer.dependentRunnableNames(MainRunnable.class.getSimpleName());
      Assert.fail("Expected IllegalArgumentException exception but got no exception");
    } catch (Exception ex) {
      Assert.assertThat(ex, CoreMatchers.instanceOf(IllegalArgumentException.class));
    }

    // test catching missing dependency
    try {
      preparer.dependentRunnableNames(MainRunnable.class.getSimpleName(), SidecarRunnable.class.getSimpleName());
      Assert.fail("Expected IllegalArgumentException exception but got no exception");
    } catch (Exception ex) {
      Assert.assertThat(ex, CoreMatchers.instanceOf(IllegalArgumentException.class));
      Assert.assertThat(ex.toString(), CoreMatchers.containsString(SidecarRunnable2.class.getSimpleName()));
    }

    // test catching missing runnable in twill specfication
    try {
      preparer.dependentRunnableNames(MainRunnable.class.getSimpleName(),
                                      SidecarRunnable.class.getSimpleName(),
                                      SidecarRunnable2.class.getSimpleName(), "missing-runnable");
      Assert.fail("Expected IllegalArgumentException exception but got no exception");
    } catch (Exception ex) {
      Assert.assertThat(ex, CoreMatchers.instanceOf(IllegalArgumentException.class));
      Assert.assertThat(ex.toString(), CoreMatchers.containsString("missing-runnable"));
    }

    // test valid dependency
    preparer.dependentRunnableNames(MainRunnable.class.getSimpleName(), SidecarRunnable.class.getSimpleName(),
                                    SidecarRunnable2.class.getSimpleName());
  }


  public static class MainRunnable extends AbstractTwillRunnable {
    @Override
    public void run() {

    }
  }

  public static class SidecarRunnable extends AbstractTwillRunnable {
    @Override
    public void run() {

    }
  }

  public static class SidecarRunnable2 extends AbstractTwillRunnable {
    @Override
    public void run() {

    }
  }
}
