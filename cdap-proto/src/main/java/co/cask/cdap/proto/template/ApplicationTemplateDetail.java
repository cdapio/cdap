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

package co.cask.cdap.proto.template;

import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.proto.ProgramType;

import java.util.Set;

/**
 * Contains detail information about an {@link ApplicationTemplate}.
 */
public class ApplicationTemplateDetail extends ApplicationTemplateMeta {

  private final Set<String> extensions;

  public ApplicationTemplateDetail(String name, String description, ProgramType programType, Set<String> extensions) {
    super(name, description, programType);
    this.extensions = extensions;
  }

  public Set<String> getExtensions() {
    return extensions;
  }
}
