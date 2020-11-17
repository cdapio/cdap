/*
 * Copyright © 2018-2020 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.util.Set;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * Tests that plugins whose requirements are marked as excluded through cdap-site.xml are excluded and is not visible
 */
public class PluginExclusionTest extends ArtifactHttpHandlerTestBase {
  @ClassRule
  public static final ExternalResource RESOURCE = new ExternalResource() {
    private String previousRequirementBlacklist;

    @Override
    protected void before() {
      // store the previous value
      previousRequirementBlacklist = System.getProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE);
      // Set the system property to the excluded requirement value which will be set in cdap-site through the test base
      System.setProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Table.TYPE);
    }

    @Override
    protected void after() {
      // clear the system property
      if (previousRequirementBlacklist == null) {
        System.clearProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE);
      } else {
        System.setProperty(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, previousRequirementBlacklist);
      }
    }
  };

  @Test
  public void testPluginRequirements() throws Exception {
    // Add a system artifact
    ArtifactId systemArtifactId = NamespaceId.SYSTEM.artifact("app", "1.0.0");
    addAppAsSystemArtifacts(systemArtifactId);

    Set<ArtifactRange> parents = Sets.newHashSet(new ArtifactRange(
      systemArtifactId.getNamespace(), systemArtifactId.getArtifact(),
      new ArtifactVersion(systemArtifactId.getVersion()), true,
      new ArtifactVersion(systemArtifactId.getVersion()), true));

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("inspection", "1.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.code(),
                        addPluginArtifact(Id.Artifact.fromEntityId(artifactId), InspectionApp.class, manifest, parents)
                          .getResponseCode());
    Set<PluginClass> plugins = getArtifactInfo(artifactId).getClasses().getPlugins();
    // Only four plugins which does not have transactions as requirement should be visible.
    Assert.assertEquals(6, plugins.size());
    Set<String> actualPluginClassNames = plugins.stream().map(PluginClass::getClassName).collect(Collectors.toSet());
    Set<String> expectedPluginClassNames = ImmutableSet.of(InspectionApp.AppPlugin.class.getName(),
                                                           InspectionApp.EmptyRequirementPlugin.class.getName(),
                                                           InspectionApp.SingleEmptyRequirementPlugin.class.getName(),
                                                           InspectionApp.NonTransactionalPlugin.class.getName(),
                                                           InspectionApp.CapabilityPlugin.class.getName(),
                                                           InspectionApp.MultipleCapabilityPlugin.class.getName());
    Assert.assertEquals(expectedPluginClassNames, actualPluginClassNames);
  }
}
