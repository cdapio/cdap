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

import { MyNamespaceApi } from 'api/namespace';
import { MyPreferenceApi } from 'api/preference';
import { MySearchApi } from 'api/search';
import { getCurrentNamespace } from 'services/NamespaceStore';
import NamespaceDetailsStore, { NamespaceDetailsActions } from 'components/NamespaceDetails/store';
import { Observable } from 'rxjs/Observable';
import { getCustomAppPipelineDatasetCounts } from 'services/metadata-parser';

function enableLoading() {
  NamespaceDetailsStore.dispatch({
    type: NamespaceDetailsActions.enableLoading,
  });
}

function getNamespacePrefs() {
  let namespace = getCurrentNamespace();

  MyPreferenceApi.getNamespacePreferences({ namespace }).subscribe(
    (res) => {
      NamespaceDetailsStore.dispatch({
        type: NamespaceDetailsActions.setData,
        payload: {
          namespacePrefs: res,
        },
      });
    },
    (err) => console.log(err)
  );
}

function getNamespaceProperties() {
  let namespace = getCurrentNamespace();

  MyNamespaceApi.get({ namespace }).subscribe(
    (res) => {
      let config = res.config;

      NamespaceDetailsStore.dispatch({
        type: NamespaceDetailsActions.setData,
        payload: {
          name: res.name,
          description: res.description,
          hdfsRootDirectory: config['root.directory'],
          hbaseNamespaceName: config['hbase.namespace'],
          hiveDatabaseName: config['hive.database'],
          schedulerQueueName: config['scheduler.queue.name'],
          principal: config.principal,
          keytabURI: config.keytabURI,
        },
      });
    },
    (err) => console.log(err)
  );
}

function getData() {
  enableLoading();

  let namespace = getCurrentNamespace();

  let searchParams = {
    namespace,
    target: ['dataset', 'application'],
    query: '*',
    sort: 'entity-name asc',
    responseFormat: 'v6',
  };

  Observable.forkJoin(
    MyNamespaceApi.get({ namespace }),
    MyPreferenceApi.getNamespacePreferences({ namespace }),
    MyPreferenceApi.getNamespacePreferencesResolved({ namespace }),
    MySearchApi.search(searchParams)
  ).subscribe(
    (res) => {
      let [namespaceInfo, namespacePrefs, resolvedPrefs, entities] = res;

      let systemPrefs = {};
      if (Object.keys(resolvedPrefs).length > Object.keys(namespacePrefs).length) {
        Object.keys(resolvedPrefs).forEach((resolvedPrefKey) => {
          if (!(resolvedPrefKey in namespacePrefs)) {
            systemPrefs[resolvedPrefKey] = resolvedPrefs[resolvedPrefKey];
          }
        });
      }

      let { pipelineCount, customAppCount, datasetCount } = getCustomAppPipelineDatasetCounts(
        entities
      );

      let config = namespaceInfo.config;

      NamespaceDetailsStore.dispatch({
        type: NamespaceDetailsActions.setData,
        payload: {
          name: namespaceInfo.name,
          description: namespaceInfo.description,
          pipelineCount,
          customAppCount,
          datasetCount,
          namespacePrefs,
          systemPrefs,
          hdfsRootDirectory: config['root.directory'],
          hbaseNamespaceName: config['hbase.namespace'],
          hiveDatabaseName: config['hive.database'],
          schedulerQueueName: config['scheduler.queue.name'],
          principal: config.principal,
          keytabURI: config.keytabURI,
        },
      });
    },
    (err) => console.log(err)
  );
}

export { getData, getNamespaceProperties, getNamespacePrefs };
