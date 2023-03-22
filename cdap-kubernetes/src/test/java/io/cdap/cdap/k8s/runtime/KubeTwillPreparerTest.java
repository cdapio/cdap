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

import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.MasterOptionConstants;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1PodSecurityContext;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.DefaultResourceSpecification;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link KubeTwillPreparer}.
 */
public class KubeTwillPreparerTest {

  private MasterEnvironmentContext createMasterEnvironmentContext() {
    return new MasterEnvironmentContext() {
      private final Map<String, String> configurations = new HashMap<>();

      @Override
      public LocationFactory getLocationFactory() {
        return null;
      }

      @Override
      public Map<String, String> getConfigurations() {
        return configurations;
      }

      @Override
      public String[] getRunnableArguments(Class<? extends MasterEnvironmentRunnable> runnableClass,
                                           String... runnableArgs) {
        return new String[0];
      }
    };
  }

  private ResourceSpecification createResourceSpecification() {
    return new DefaultResourceSpecification(1, 100,
                                            1, 1, 1);
  }

  private TwillSpecification createTwillSpecification() throws Exception {
    return TwillSpecification.Builder.with()
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
  }

  private PodInfo createPodInfo() {
    return new PodInfo("test-pod-name", "test-pod-dir", "test-label-file.txt",
                       "test-name-file.txt", "test-pod-uid", "test-uid-file.txt", "test-namespace-file.txt",
                       "test-pod-namespace", Collections.emptyMap(), Collections.emptyList(),
                       "test-pod-service-account", "test-pod-runtime-class",
                       Collections.emptyList(), "test-pod-container-label", "test-pod-container-image",
                       Collections.emptyList(), Collections.emptyList(), new V1PodSecurityContext(),
                       "test-pod-image-pull-policy");
  }

  @Test
  public void testWithDependentRunnables() throws Exception {
    KubeTwillPreparer preparer = new KubeTwillPreparer(createMasterEnvironmentContext(), null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
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

  @Test
  public void testCreateDefaultResourceSpecification() throws Exception {
    KubeTwillPreparer preparer = new KubeTwillPreparer(createMasterEnvironmentContext(), null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "system");
    preparer.withConfiguration(config);
    V1ResourceRequirements gotResourceRequirements = preparer.createResourceRequirements(resourceSpecification);
    Assert.assertEquals("1", gotResourceRequirements.getRequests().get("cpu").toSuffixedString());
    Assert.assertEquals("100Mi", gotResourceRequirements.getRequests().get("memory").toSuffixedString());
  }

  @Test
  public void testCreateDefaultSystemResourceSpecification() throws Exception {
    KubeTwillPreparer preparer = new KubeTwillPreparer(createMasterEnvironmentContext(), null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "system");
    preparer.withConfiguration(config);

    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    V1ResourceRequirements gotResourceRequirements = preparer.createResourceRequirements(resourceSpecification);
    Assert.assertEquals("1", gotResourceRequirements.getRequests().get("cpu").toSuffixedString());
    Assert.assertEquals("100Mi", gotResourceRequirements.getRequests().get("memory").toSuffixedString());
  }

  @Test
  public void testOwnerReferencesNotSetOnUserRuns() throws Exception {
    V1OwnerReference ownerReference = new V1OwnerReference()
        .apiVersion("apps/v1")
        .kind("ReplicaSet")
        .name("system-worker");
    PodInfo podInfo = new PodInfo("test-pod-name", "test-pod-dir", "test-label-file.txt",
        "test-name-file.txt", "test-pod-uid", "test-uid-file.txt", "test-namespace-file.txt",
        "test-pod-namespace", Collections.emptyMap(), Collections.singletonList(ownerReference),
        "test-pod-service-account", "test-pod-runtime-class",
        Collections.emptyList(), "test-pod-container-label", "test-pod-container-image",
        Collections.emptyList(), Collections.emptyList(), new V1PodSecurityContext(),
        "test-pod-image-pull-policy");

    KubeTwillPreparer preparer = new KubeTwillPreparer(createMasterEnvironmentContext(),
        null, "default", podInfo, createTwillSpecification(),
        null, null, null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "ns1");
    preparer.withConfiguration(config);

    V1ObjectMeta objectMeta = preparer.createResourceMetadata(V1Job.class, "runnable", 0, true);
    Assert.assertNull(objectMeta.getOwnerReferences());
  }

  @Test
  public void testCreateResourceSpecificationWithCustomResourceMultipliers() throws Exception {
    MasterEnvironmentContext masterEnvironmentContext = createMasterEnvironmentContext();
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.CPU_MULTIPLIER, "0.5");
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.MEMORY_MULTIPLIER, "0.25");
    KubeTwillPreparer preparer = new KubeTwillPreparer(masterEnvironmentContext, null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "system");
    preparer.withConfiguration(config);
    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    V1ResourceRequirements gotResourceRequirements = preparer.createResourceRequirements(resourceSpecification);
    Assert.assertEquals("500m", gotResourceRequirements.getRequests().get("cpu").toSuffixedString());
    Assert.assertEquals("25Mi", gotResourceRequirements.getRequests().get("memory").toSuffixedString());
  }

  @Test
  public void testCreateDefaultUserResourceSpecification() throws Exception {
    MasterEnvironmentContext masterEnvironmentContext = createMasterEnvironmentContext();
    KubeTwillPreparer preparer = new KubeTwillPreparer(masterEnvironmentContext, null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "non-system-namespace");
    preparer.withConfiguration(config);

    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    V1ResourceRequirements gotResourceRequirements = preparer.createResourceRequirements(resourceSpecification);
    Assert.assertEquals("500m", gotResourceRequirements.getRequests().get("cpu").toSuffixedString());
    Assert.assertEquals("50Mi", gotResourceRequirements.getRequests().get("memory").toSuffixedString());
    Assert.assertEquals("1", gotResourceRequirements.getLimits().get("cpu").toSuffixedString());
    Assert.assertEquals("100Mi", gotResourceRequirements.getLimits().get("memory").toSuffixedString());
  }

  @Test
  public void testCreateUserResourceSpecificationWithCustomResourceMultipliers() throws Exception {
    MasterEnvironmentContext masterEnvironmentContext = createMasterEnvironmentContext();
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.PROGRAM_CPU_MULTIPLIER, "0.3");
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.PROGRAM_MEMORY_MULTIPLIER, "0.7");
    KubeTwillPreparer preparer = new KubeTwillPreparer(masterEnvironmentContext, null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "non-system-namespace");
    preparer.withConfiguration(config);

    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    V1ResourceRequirements gotResourceRequirements = preparer.createResourceRequirements(resourceSpecification);
    Assert.assertEquals("300m", gotResourceRequirements.getRequests().get("cpu").toSuffixedString());
    Assert.assertEquals("70Mi", gotResourceRequirements.getRequests().get("memory").toSuffixedString());
    Assert.assertEquals("1", gotResourceRequirements.getLimits().get("cpu").toSuffixedString());
    Assert.assertEquals("100Mi", gotResourceRequirements.getLimits().get("memory").toSuffixedString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateUserResourceSpecificationInvalidProgramCPUMultiplier() throws Exception {
    MasterEnvironmentContext masterEnvironmentContext = createMasterEnvironmentContext();
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.PROGRAM_CPU_MULTIPLIER, "2");
    KubeTwillPreparer preparer = new KubeTwillPreparer(masterEnvironmentContext, null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "non-system-namespace");
    preparer.withConfiguration(config);

    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    preparer.createResourceRequirements(resourceSpecification);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateUserResourceSpecificationInvalidProgramMemoryMultiplier() throws Exception {
    MasterEnvironmentContext masterEnvironmentContext = createMasterEnvironmentContext();
    masterEnvironmentContext.getConfigurations().put(KubeTwillPreparer.PROGRAM_MEMORY_MULTIPLIER, "2");
    KubeTwillPreparer preparer = new KubeTwillPreparer(masterEnvironmentContext, null, "default",
                                                       createPodInfo(), createTwillSpecification(), null, null,
                                                       null, null, null);
    Map<String, String> config = new HashMap<>();
    config.put(MasterOptionConstants.RUNTIME_NAMESPACE, "non-system-namespace");
    preparer.withConfiguration(config);

    ResourceSpecification resourceSpecification = new DefaultResourceSpecification(1, 100, 1, 1, 1);
    preparer.createResourceRequirements(resourceSpecification);
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
