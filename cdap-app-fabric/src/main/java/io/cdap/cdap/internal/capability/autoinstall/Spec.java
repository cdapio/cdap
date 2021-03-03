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

package io.cdap.cdap.internal.capability.autoinstall;

import java.util.List;
import java.util.Objects;

/**
 * Represents spec.json describing an artifact on hub.
 * See https://cdap.atlassian.net/wiki/spaces/DOCS/pages/554401840/Hub+API?src=search#Get-Package-Specification
 */
public class Spec {
  private final String specVersion;
  private final String label;
  private final String description;
  private final String author;
  private final String org;
  private final long created;
  private final String cdapVersion;
  private final List<String> categories;
  private final boolean preview;
  private final List<Action> actions;

  public Spec(String specVersion, String label, String description, String author, String org, long created,
              String cdapVersion, List<String> categories, boolean preview, List<Action> actions) {
    this.specVersion = specVersion;
    this.label = label;
    this.description = description;
    this.author = author;
    this.org = org;
    this.created = created;
    this.cdapVersion = cdapVersion;
    this.categories = categories;
    this.actions = actions;
    this.preview = preview;
  }

  public String getSpecVersion() {
    return specVersion;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public String getAuthor() {
    return author;
  }

  public String getOrg() {
    return org;
  }

  public long getCreated() {
    return created;
  }

  public String getCdapVersion() {
    return cdapVersion;
  }

  public List<String> getCategories() {
    return categories;
  }

  public boolean getPreview() {
    return preview;
  }

  public List<Action> getActions() {
    return actions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Spec that = (Spec) o;
    return Objects.equals(specVersion, that.specVersion)
      && Objects.equals(label, that.label)
      && Objects.equals(description, that.description)
      && Objects.equals(author, that.author)
      && Objects.equals(org, that.org)
      && Objects.equals(created, that.created)
      && Objects.equals(cdapVersion, that.cdapVersion)
      && Objects.equals(categories, that.categories)
      && Objects.equals(preview, that.preview)
      && Objects.equals(actions, that.actions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(specVersion, label, description, author, org, created,
                        cdapVersion, categories, preview, actions);
  }

  /**
   * Represents an action in spec.json.
   */
  public static class Action {
    private final String type;
    private final String label;
    private final List<Argument> arguments;

    public Action(String type, String label, List<Argument> arguments) {
      this.type = type;
      this.label = label;
      this.arguments = arguments;
    }

    public String getType() {
      return type;
    }

    public String getLabel() {
      return label;
    }

    public List<Argument> getArguments() {
      return arguments;
    }

    public String getJarName() {
      for (Argument argument: arguments) {
        if (argument.getName().equals("jar")) {
          return argument.getValue();
        }
      }
      return null;
    }

    public String getConfigFilename() {
      for (Argument argument: arguments) {
        if (argument.getName().equals("config")) {
          return argument.getValue();
        }
      }
      return null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Action that = (Action) o;
      return Objects.equals(type, that.type)
        && Objects.equals(label, that.label)
        && Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, label, arguments);
    }

    /**
     * Represents an action argument in spec.json.
     */
    public static class Argument {
      private final String name;
      private final String value;
      private final Boolean canModify;

      public Argument(String name, String value, Boolean canModify) {
        this.name = name;
        this.value = value;
        this.canModify = canModify;
      }

      public String getName() {
        return name;
      }

      public String getValue() {
        return value;
      }

      public Boolean getCanModify() {
        return canModify;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }

        Argument that = (Argument) o;
        return Objects.equals(name, that.name)
          && Objects.equals(value, that.value)
          && Objects.equals(canModify, that.canModify);
      }

      @Override
      public int hashCode() {
        return Objects.hash(name, value, canModify);
      }
    }
  }
}
