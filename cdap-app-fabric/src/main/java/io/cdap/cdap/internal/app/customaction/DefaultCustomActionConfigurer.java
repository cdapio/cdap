/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.customaction;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.customaction.CustomActionConfigurer;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.AbstractConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.customaction.DefaultCustomActionSpecification;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.internal.specification.DataSetFieldExtractor;
import io.cdap.cdap.internal.specification.PropertyFieldExtractor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of the {@link CustomActionConfigurer}.
 */
public final class DefaultCustomActionConfigurer extends AbstractConfigurer implements CustomActionConfigurer {

  private final CustomAction customAction;

  private String name;
  private String description;
  private Map<String, String> properties;

  private DefaultCustomActionConfigurer(CustomAction customAction, Id.Namespace deployNamespace, Id.Artifact artifactId,
                                        ArtifactRepository artifactRepository, PluginInstantiator pluginInstantiator,
                                        PluginFinder pluginFinder) {
    super(deployNamespace, artifactId, artifactRepository, pluginInstantiator, pluginFinder);
    this.customAction = customAction;
    this.name = customAction.getClass().getSimpleName();
    this.description = "";
    this.properties = new HashMap<>();
  }

  @Override
  public void setName(String name) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Name of the CustomAction cannot be null or empty");
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    Preconditions.checkNotNull(description, "Description of the CustomAction cannot be null");
    this.description = description;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties of the CustomAction cannot be null");
    this.properties = new HashMap<>(properties);
  }

  private DefaultCustomActionSpecification createSpecification() {
    Set<String> datasets = new HashSet<>();
    Reflections.visit(customAction, customAction.getClass(), new PropertyFieldExtractor(properties),
                      new DataSetFieldExtractor(datasets));
    return new DefaultCustomActionSpecification(customAction.getClass().getName(), name,
                                                description, properties, datasets);
  }

  public static CustomActionSpecification configureAction(CustomAction action, Id.Namespace deployNamespace,
                                                          Id.Artifact artifactId, ArtifactRepository artifactRepository,
                                                          PluginInstantiator pluginInstantiator, PluginFinder pluginFinder) {
    DefaultCustomActionConfigurer configurer = new DefaultCustomActionConfigurer(action, deployNamespace, artifactId,
                                                                                 artifactRepository,
                                                                                 pluginInstantiator, null);
    action.configure(configurer);
    return configurer.createSpecification();
  }
}
