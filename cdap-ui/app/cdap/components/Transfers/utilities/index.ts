/*
 * Copyright © 2019 Cask Data, Inc.
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

import { MyProgramApi } from 'api/program';
import { MyAppApi } from 'api/app';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Observable } from 'rxjs/Observable';
import { MyPipelineApi } from 'api/pipeline';
import { MyDeltaApi } from 'api/delta';

// TODO: modify constants to the correct one once backend app is ready
const programType = 'workers';
const programId = 'DeltaWorker';
const batchProgramType = 'Worker';
const parentArtifact = 'delta-app';
const version = '0.1.0-SNAPSHOT';
const scope = 'SYSTEM';

export function start(transfer, successCb, errorCb) {
  const params = {
    namespace: getCurrentNamespace(),
    appId: transfer.name,
    programType,
    programId,
    action: 'start',
  };

  MyProgramApi.action(params).subscribe(successCb, errorCb);
}

export function stop(transfer, successCb, errorCb) {
  const params = {
    namespace: getCurrentNamespace(),
    appId: transfer.name,
    programType,
    programId,
    action: 'stop',
  };

  MyProgramApi.action(params).subscribe(successCb, errorCb);
}

export function deleteApp(transfer, successCb, errorCb) {
  const params = {
    namespace: getCurrentNamespace(),
    appId: `CDC-${transfer.id}`,
  };

  const deltaParams = {
    context: getCurrentNamespace(),
    id: transfer.id,
  };

  MyAppApi.delete(params)
    .combineLatest(MyDeltaApi.delete(deltaParams))
    .subscribe(successCb, errorCb);
}

export function getStatuses(list) {
  const params = {
    namespace: getCurrentNamespace(),
  };

  const body = list.map((transfer) => {
    return {
      appId: transfer.name,
      programType: batchProgramType,
      programId,
    };
  });

  return MyAppApi.batchStatus(params, body);
}

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

export function createTransfer(name, description, source, target) {
  const transferSpec = {
    artifact: {
      name: parentArtifact,
      version,
      scope,
    },
    name,
    description,
    config: {
      connections: [
        {
          from: source.name,
          to: target.name,
        },
      ],
      stages: [source, target],
      offsetBasePath: '/tmp/delta',
    },
  };

  const params = {
    namespace: getCurrentNamespace(),
    appId: name,
  };

  return MyPipelineApi.publish(params, transferSpec);
}
