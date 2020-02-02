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
    const pluginParams = {
      namespace: getCurrentNamespace(),
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
      ([res]) => {
        observer.next(res);
        observer.complete();
      },
      (err) => {
        observer.error(err);
      }
    );
  });

  return observable$;
}

export function fetchPluginWidget(
  artifactName,
  artifactVersion,
  artifactScope,
  pluginName,
  pluginType
) {
  const observable$ = Observable.create((observer) => {
    const widgetKey = `widgets.${pluginName}-${pluginType}`;
    const params = {
      namespace: getCurrentNamespace(),
      artifactName,
      artifactVersion,
      scope: artifactScope,
      keys: widgetKey,
    };

    MyPipelineApi.fetchWidgetJson(params).subscribe(
      (res) => {
        if (!res || !res[widgetKey]) {
          observer.next({});
          observer.complete();
          return;
        }

        try {
          const widgetContent = JSON.parse(res[widgetKey]);

          observer.next(widgetContent);

          observer.complete();
        } catch (parseError) {
          observer.error(parseError);
        }
      },
      (err) => {
        observer.error(err);
      }
    );
  });

  return observable$;
}

// export function fetchPluginInfo(artifactName, artifactScope, pluginName, pluginType) {
//   const observable$ = Observable.create((observer) => {
//     const namespace = getCurrentNamespace();
//     const pluginParams = {
//       namespace,
//       parentArtifact,
//       version,
//       extension: pluginType,
//       pluginName,
//       scope,
//       artifactName,
//       artifactScope,
//       limit: 1,
//       order: 'DESC',
//     };

//     MyPipelineApi.getPluginProperties(pluginParams).subscribe(
//       ([plugin]) => {
//         const widgetKey = `widgets.${pluginName}-${pluginType}`;
//         const widgetParams = {
//           namespace,
//           artifactName,
//           scope: artifactScope,
//           artifactVersion: plugin.artifact.version,
//           keys: widgetKey,
//         };

//         MyPipelineApi.fetchWidgetJson(widgetParams).subscribe(
//           (widgetInfo) => {
//             if (!widgetInfo || !widgetInfo[widgetKey]) {
//               observer.next({
//                 pluginInfo: plugin,
//                 widgetInfo: {},
//               });
//               observer.complete();
//               return;
//             }

//             try {
//               const widgetContent = JSON.parse(widgetInfo[widgetKey]);

//               observer.next({
//                 pluginInfo: plugin,
//                 widgetInfo: widgetContent,
//               });

//               observer.complete();
//             } catch (parseError) {
//               observer.error(parseError);
//             }
//           },
//           (widgetError) => {
//             observer.error(widgetError);
//           }
//         );
//       },
//       (pluginError) => {
//         observer.error(pluginError);
//       }
//     );
//   });

//   return observable$;
// }

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
  sourcePluginInfo,
  targetPluginInfo,
  sourceConfig,
  targetConfig
) {
  const source = constructPluginConfigurationSpec(sourcePluginInfo, sourceConfig);
  const target = constructPluginConfigurationSpec(targetPluginInfo, targetConfig);

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
          from: source.name,
          to: target.name,
        },
      ],
      stages: [source, target],
      offsetBasePath: '/tmp/Replicator',
    },
  };

  return transferSpec;
}
