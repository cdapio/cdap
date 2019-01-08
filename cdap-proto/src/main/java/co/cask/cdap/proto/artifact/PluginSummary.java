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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.artifact.ArtifactSummary;

import java.util.Objects;

/**
 * Represents an plugin info returned by
 * /artifacts/{artifact-name}/versions/{artifact-version}/extensions/{plugin-type}
 */
@Beta
public class PluginSummary {
    protected final String name;
    protected final String type;
    protected final String description;
    protected final String[] pluginInput;
    protected final String[] pluginOutput;
    protected final String[] pluginFunction;
    protected final String className;
    protected final ArtifactSummary artifact;

    public PluginSummary(String name, String type, String description, String[] pluginInput, String[] pluginOutput,
            String[] pluginFunction, String className, ArtifactSummary artifact) {
        this.name = name;
        this.type = type;
        this.description = description;
        this.pluginInput = pluginInput;
        this.pluginOutput = pluginOutput;
        this.pluginFunction = pluginFunction;
        this.className = className;
        this.artifact = artifact;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public String getClassName() {
        return className;
    }

    public String[] getPluginInput() {
        return pluginInput;
    }

    public String[] getPluginOutput() {
        return pluginOutput;
    }

    public String[] getPluginFunction() {
        return pluginFunction;
    }

    public String getPluginInputToString() {
        if (pluginInput == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String info : pluginInput) {
            sb.append(info);
            sb.append(",");
        }
        return sb.toString();
    }

    public String getPluginOutputToString() {
        if (pluginOutput == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String info : pluginOutput) {
            sb.append(info);
            sb.append(",");
        }
        return sb.toString();
    }

    public String getPluginFunctionToString() {
        if (pluginFunction == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String info : pluginFunction) {
            sb.append(info);
            sb.append(",");
        }
        return sb.toString();
    }

    public ArtifactSummary getArtifact() {
        return artifact;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PluginSummary that = (PluginSummary) o;

        return Objects.equals(name, that.name) && Objects.equals(type, that.type)
                && Objects.equals(description, that.description) && Objects.equals(pluginFunction, that.pluginFunction)
                && Objects.equals(pluginInput, that.pluginInput) && Objects.equals(pluginOutput, that.pluginOutput)
                && Objects.equals(className, that.className) && Objects.equals(artifact, that.artifact);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description, pluginFunction, pluginInput, pluginOutput, className, artifact);
    }

    @Override
    public String toString() {
        return "PluginSummary{" + "name='" + name + '\'' + ", type='" + type + '\'' + ", description='" + description
                + '\'' + ", pluginFunction='" + getPluginFunctionToString() + '\'' + ", pluginInput='"
                + getPluginInputToString() + '\'' + ", pluginOutput='" + getPluginOutputToString() + '\''
                + ", className='" + className + '\'' + ", artifact=" + artifact + '}';
    }
}
