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
import ee from 'event-emitter';
import {orderBy, find} from 'lodash';

let workspaceRetries;

export function execute(addDirective, shouldReset, hideLoading = false) {
  let eventEmitter = ee(ee);
  eventEmitter.emit('CLOSE_POPOVER');
  if (!hideLoading) {
    DataPrepStore.dispatch({
      type: DataPrepActions.enableLoading
    });
  }

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
            directives: updatedDirectives,
            types: res.types
          }
        });

        fetchColumnsInformation(params, requestBody, res.header);
      }, (err) => {
        observer.onError(err);
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading
        });
      });
  });
}

function setWorkspaceRetry(params, observer, workspaceId) {
  MyDataPrepApi.getWorkspace(params)
    .subscribe((res) => {
      let {dataprep} = DataPrepStore.getState();
      /*
        1. Open a tab with huge data (like 400 columns and 100 rows)
        2. Change of mind, open another tab
        3. 2nd tab's data comes in quick and you are happy browsing it
        4. Baam 1st tab's data comes and royally overwrites the one you are seeing.

        This is to prevent that. We can't cancel the request we made but we should show only that is relevant
      */
      if (dataprep.workspaceId !== workspaceId) {
        return;
      }
      let directives = objectQuery(res, 'values', '0', 'recipe', 'directives') || [];
      let requestBody = directiveRequestBodyCreator(directives);

      let workspaceUri = objectQuery(res, 'values', '0', 'properties', 'path');
      let workspaceInfo = objectQuery(res, 'values', '0');

      MyDataPrepApi.execute(params, requestBody)
        .subscribe((response) => {
          observer.onNext(response);

          DataPrepStore.dispatch({
            type: DataPrepActions.setWorkspace,
            payload: {
              data: response.values,
              headers: response.header,
              types: response.types,
              directives,
              workspaceId,
              workspaceUri,
              workspaceInfo
            }
          });

          fetchColumnsInformation(params, requestBody, response.header);

        }, (err) => {
          // Backend returned an exception. Show default error message for now able to show data.
          if (workspaceRetries < 3) {
            workspaceRetries += 1;
            setWorkspaceRetry(params, observer, workspaceId);
          } else {
            observer.onNext(err);
            DataPrepStore.dispatch({
              type: DataPrepActions.disableLoading
            });
            DataPrepStore.dispatch({
              type: DataPrepActions.setDataError,
              payload: {
                errorMessage: true
              }
            });
          }
        });

    }, (err) => {
      if (workspaceRetries < 3) {
        workspaceRetries += 1;
        setWorkspaceRetry(params, observer, workspaceId);
      } else {
        DataPrepStore.dispatch({
          type: DataPrepActions.setDataError,
          payload: {
            errorMessage: true
          }
        });
        observer.onError(err);
      }
    });
}

export function setWorkspace(workspaceId) {
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    namespace,
    workspaceId
  };

  DataPrepStore.dispatch({
    type: DataPrepActions.setWorkspaceId,
    payload: {
      workspaceId,
      loading: true
    }
  });

  workspaceRetries = 0;

  return Rx.Observable.create((observer) => {
    setWorkspaceRetry(params, observer, workspaceId);
  });
}

function fetchColumnsInformation(params, requestBody, headers) {
  MyDataPrepApi.summary(params, requestBody)
    .subscribe((summaryRes) => {
      let columns = {};

      headers.forEach((head) => {
        columns[head] = {
          general: objectQuery(summaryRes, 'values', 'statistics', head, 'general'),
          types: objectQuery(summaryRes, 'values', 'statistics', head, 'types'),
          isValid: objectQuery(summaryRes, 'values', 'validation', head, 'valid')
        };
      });

      DataPrepStore.dispatch({
        type: DataPrepActions.setColumnsInformation,
        payload: {
          columns
        }
      });

    }, (err) => {
      console.log('error fetching summary', err);
    });
}

export function getWorkspaceList(workspaceId) {
  let namespace = NamespaceStore.getState().selectedNamespace;

  MyDataPrepApi.getWorkspaceList({ namespace })
    .subscribe((res) => {
      if (res.values.length === 0) {
        DataPrepStore.dispatch({
          type: DataPrepActions.setWorkspaceList,
          payload: {
            list: []
          }
        });

        return;
      }

      let workspaceList = orderBy(res.values, [(workspace) => workspace.name.toLowerCase()], ['asc']);

      DataPrepStore.dispatch({
        type: DataPrepActions.setWorkspaceList,
        payload: {
          list: workspaceList
        }
      });

      if (workspaceId) {
        // Set active workspace
        // Check for existance of the workspaceId
        let workspaceObj = find(workspaceList, { id: workspaceId });

        let workspaceStream;
        if (workspaceObj) {
          workspaceStream = setWorkspace(workspaceId);
        } else {
          workspaceStream = setWorkspace(workspaceList[0].id);
        }

        workspaceStream.subscribe();
      }
    });
}
