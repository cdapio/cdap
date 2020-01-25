/*
 * Copyright © 2020 Cask Data, Inc.
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

import { getCurrentNamespace } from 'services/NamespaceStore';
import { Observable } from 'rxjs/Observable';
import { MyPipelineApi } from 'api/pipeline';

const parentArtifact = 'delta-app';
const version = '0.1.0-SNAPSHOT';
const scope = 'SYSTEM';

export function fetchPluginInfo(artifactName, artifactScope, pluginName, pluginType) {
  const observable$ = Observable.create((observer) => {
    const namespace = getCurrentNamespace();
    const pluginParams = {
      namespace,
      parentArtifact,
      version,
      extension: pluginType,
      pluginName,
      scope,
      artifactName,
      artifactScope,
      limit: 1,
      order: 'DESC',
    };

    MyPipelineApi.getPluginProperties(pluginParams).subscribe(
      ([plugin]) => {
        const widgetKey = `widgets.${pluginName}-${pluginType}`;
        const widgetParams = {
          namespace,
          artifactName,
          scope: artifactScope,
          artifactVersion: plugin.artifact.version,
          keys: widgetKey,
        };

        MyPipelineApi.fetchWidgetJson(widgetParams).subscribe(
          (widgetInfo) => {
            if (!widgetInfo || !widgetInfo[widgetKey]) {
              observer.next({
                pluginInfo: plugin,
                widgetInfo: {},
              });
              observer.complete();
              return;
            }

            try {
              const widgetContent = JSON.parse(widgetInfo[widgetKey]);

              observer.next({
                pluginInfo: plugin,
                widgetInfo: widgetContent,
              });

              observer.complete();
            } catch (parseError) {
              observer.error(parseError);
            }
          },
          (widgetError) => {
            observer.error(widgetError);
          }
        );
      },
      (pluginError) => {
        observer.error(pluginError);
      }
    );
  });

  return observable$;
}

function constructPluginConfigurationSpec(plugin, pluginConfig) {
  return {
    name: plugin.name,
    plugin: {
      name: plugin.name,
      type: plugin.type,
      artifact: {
        ...plugin.artifact,
      },
      properties: {
        ...pluginConfig,
      },
    },
  };
}

export function constructReplicatorSpec(
  name,
  description,
  sourcePlugin,
  targetPlugin,
  sourceConfig,
  targetConfig
) {
  const sourceInfo = sourcePlugin.pluginInfo;
  const targetInfo = targetPlugin.pluginInfo;

  const transferSpec = {
    name,
    description,
    artifact: {
      name: parentArtifact,
      version,
      scope,
    },
    config: {
      connections: [
        {
          from: sourceInfo.name,
          to: targetInfo.name,
        },
      ],
      stages: [
        constructPluginConfigurationSpec(sourceInfo, sourceConfig),
        constructPluginConfigurationSpec(targetInfo, targetConfig),
      ],
      offsetBasePath: '/tmp/Replicator',
    },
  };

  return transferSpec;
}
