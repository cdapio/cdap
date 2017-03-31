/*
 * Copyright © 2017 Cask Data, Inc.
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

import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import Rx from 'rx';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import {objectQuery} from 'services/helpers';

export function execute(addDirective, shouldReset) {
  DataPrepStore.dispatch({
    type: DataPrepActions.enableLoading
  });

  let store = DataPrepStore.getState().dataprep;
  let updatedDirectives = store.directives.concat(addDirective);

  if (shouldReset) {
    updatedDirectives = addDirective;
  }

  let workspaceId = store.workspaceId;
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    namespace,
    workspaceId
  };

  let requestBody = directiveRequestBodyCreator(updatedDirectives);

  return Rx.Observable.create((observer) => {
    MyDataPrepApi.execute(params, requestBody)
      .subscribe((res) => {
        observer.onNext(res);

        DataPrepStore.dispatch({
          type: DataPrepActions.setDirectives,
          payload: {
            data: res.values,
            headers: res.header,
            directives: updatedDirectives
          }
        });
      }, (err) => {
        observer.onError(err);
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading
        });
      });
  });
}

export function setWorkspace(workspaceId) {
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    namespace,
    workspaceId
  };

  return Rx.Observable.create((observer) => {
    MyDataPrepApi.getWorkspace(params)
      .subscribe((res) => {
        let directives = objectQuery(res, 'values', '0', 'recipe', 'directives') || [];
        let requestBody = directiveRequestBodyCreator(directives);

        MyDataPrepApi.execute(params, requestBody)
          .subscribe((res) => {
            observer.onNext(res);

            DataPrepStore.dispatch({
              type: DataPrepActions.setWorkspace,
              payload: {
                data: res.values,
                headers: res.header,
                directives,
                workspaceId
              }
            });
          }, (err) => {
            // This flow is because of the workspace is empty

            observer.onNext(err);
            DataPrepStore.dispatch({
              type: DataPrepActions.setWorkspace,
              payload: {
                data: [],
                headers: [],
                workspaceId
              }
            });
          });

      }, (err) => {
        console.log('get workspace err', err);
        observer.onError(err);
      });
  });
}
