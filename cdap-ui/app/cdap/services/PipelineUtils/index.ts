/*
 * Copyright © 2018 Cask Data, Inc.
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
import { MyPipelineApi } from 'api/pipeline';
import { Observable } from 'rxjs/Observable';

interface IPipelineConfig {
  name: string;
  description: string;
  artifact: {
    name: string;
    version: string;
    scope: string;
  };
  config: any;
}

export function duplicatePipeline(pipelineName: string, config?: IPipelineConfig): void {
  const newName = getClonePipelineName(pipelineName);

  if (config) {
    setConfigAndNavigate({ ...config, name: newName });
    return;
  }

  getPipelineConfig(pipelineName).subscribe((pipelineConfig) => {
    setConfigAndNavigate({ ...pipelineConfig, name: newName });
  });
}

export function getPipelineConfig(pipelineName: string): Observable<IPipelineConfig> {
  return Observable.create((observer) => {
    const params = {
      namespace: getCurrentNamespace(),
      appId: pipelineName,
    };

    MyPipelineApi.get(params).subscribe((res) => {
      const pipelineConfig = {
        name: res.name,
        description: res.description,
        artifact: res.artifact,
        config: JSON.parse(res.configuration),
      };

      observer.next(pipelineConfig);
      observer.complete();
    });
  });
}

function setConfigAndNavigate(config: IPipelineConfig): void {
  window.localStorage.setItem(config.name, JSON.stringify(config));

  const hydratorLink = window.getHydratorUrl({
    stateName: 'hydrator.create',
    stateParams: {
      namespace: getCurrentNamespace(),
      cloneId: config.name,
      artifactType: config.artifact.name,
    },
  });

  window.location.href = hydratorLink;
}

function getClonePipelineName(name: string): string {
  const match = name.match(/(_v[\d]*)$/g);
  let version;
  let existingSuffix;

  if (Array.isArray(match)) {
    version = match.pop();
    existingSuffix = version;
    version = version.replace('_v', '');
    version = '_v' + ((!isNaN(parseInt(version, 10)) ? parseInt(version, 10) : 1) + 1);
  } else {
    version = '_v1';
  }

  return name.split(existingSuffix)[0] + version;
}
