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
import { Observable } from 'rxjs/Observable';
import { directiveRequestBodyCreator } from 'components/DataPrep/helper';
import { objectQuery } from 'services/helpers';
import ee from 'event-emitter';
import { orderBy, find } from 'lodash';
import { Theme } from 'services/ThemeHelper';

let workspaceRetries;

export function execute(addDirective, shouldReset, hideLoading = false) {
  let eventEmitter = ee(ee);
  eventEmitter.emit('CLOSE_POPOVER');
  if (!hideLoading) {
    DataPrepStore.dispatch({
      type: DataPrepActions.enableLoading,
    });
  }

  let store = DataPrepStore.getState().dataprep;
  let updatedDirectives = store.directives.concat(addDirective);

  if (shouldReset) {
    updatedDirectives = addDirective;
  }

  let workspaceId = store.workspaceId;
  let properties = store.properties;
  /*
      This is because everytime we change the data there is a possibility that we
      change the schema and with schema change the visualization is not guaranteed
      to be correct. For now we just clear it. We should become smart enough to say if the
      visualization is still good enough (with just data change)
  */
  properties.visualization = {};
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    context: namespace,
    workspaceId,
  };

  let requestBody = directiveRequestBodyCreator(updatedDirectives);
  requestBody.properties = properties;

  return Observable.create((observer) => {
    MyDataPrepApi.execute(params, requestBody).subscribe(
      (res) => {
        observer.next(res);

        DataPrepStore.dispatch({
          type: DataPrepActions.setDirectives,
          payload: {
            data: res.values,
            headers: res.header,
            directives: res.directives,
            types: res.types,
          },
        });

        fetchColumnsInformation(params, requestBody, res.header);
      },
      (err) => {
        observer.error(err);
        DataPrepStore.dispatch({
          type: DataPrepActions.disableLoading,
        });
      }
    );
  });
}

function setWorkspaceRetry(params, observer, workspaceId) {
  MyDataPrepApi.getWorkspace(params).subscribe(
    (res) => {
      let { dataprep } = DataPrepStore.getState();
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
      let properties = objectQuery(res, 'values', 0, 'properties');
      requestBody.properties = properties;

      let workspaceUri = objectQuery(res, 'values', '0', 'properties', 'path');
      let workspaceInfo = objectQuery(res, 'values', '0');

      MyDataPrepApi.execute(params, requestBody).subscribe(
        (response) => {
          DataPrepStore.dispatch({
            type: DataPrepActions.setWorkspace,
            payload: {
              data: response.values,
              headers: response.header,
              types: response.types,
              directives,
              workspaceId,
              workspaceUri,
              workspaceInfo,
              properties,
            },
          });

          observer.next(response);
          fetchColumnsInformation(params, requestBody, response.header);
        },
        (err) => {
          // Backend returned an exception. Show default error message for now able to show data.
          if (workspaceRetries < 3) {
            workspaceRetries += 1;
            setWorkspaceRetry(params, observer, workspaceId);
          } else {
            observer.next(err);
            DataPrepStore.dispatch({
              type: DataPrepActions.disableLoading,
            });
            DataPrepStore.dispatch({
              type: DataPrepActions.setDataError,
              payload: {
                errorMessage: true,
              },
            });
          }
        }
      );
    },
    (err) => {
      if (workspaceRetries < 3) {
        workspaceRetries += 1;
        setWorkspaceRetry(params, observer, workspaceId);
      } else {
        DataPrepStore.dispatch({
          type: DataPrepActions.setWorkspaceError,
          payload: {
            workspaceError: { message: err.response.message, statusCode: err.statusCode },
          },
        });
        observer.error(err);
      }
    }
  );
}

export function updateWorkspaceProperties() {
  let { directives, workspaceId, properties } = DataPrepStore.getState().dataprep;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let params = {
    context: namespace,
    workspaceId,
  };
  let requestBody = directiveRequestBodyCreator(directives);
  requestBody.properties = properties;
  MyDataPrepApi.execute(params, requestBody).subscribe(
    () => {},
    (err) => console.log('Error updating workspace visualization: ', err)
  );
}
function checkAndUpdateExistingWorkspaceProperties() {
  let { workspaceId, workspaceInfo } = DataPrepStore.getState().dataprep;
  if (!workspaceId || !workspaceInfo) {
    return;
  }
  updateWorkspaceProperties();
}
export function setWorkspace(workspaceId) {
  checkAndUpdateExistingWorkspaceProperties();
  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    context: namespace,
    workspaceId,
  };

  DataPrepStore.dispatch({
    type: DataPrepActions.setWorkspaceId,
    payload: {
      workspaceId,
      loading: true,
    },
  });

  workspaceRetries = 0;

  return Observable.create((observer) => {
    setWorkspaceRetry(params, observer, workspaceId);
  });
}

function fetchColumnsInformation(params, requestBody, headers) {
  MyDataPrepApi.summary(params, requestBody).subscribe(
    (summaryRes) => {
      let columns = {};

      headers.forEach((head) => {
        columns[head] = {
          general: objectQuery(summaryRes, 'values', 'statistics', head, 'general'),
          types: objectQuery(summaryRes, 'values', 'statistics', head, 'types'),
          isValid: objectQuery(summaryRes, 'values', 'validation', head, 'valid'),
        };
      });

      DataPrepStore.dispatch({
        type: DataPrepActions.setColumnsInformation,
        payload: {
          columns,
        },
      });
    },
    (err) => {
      console.log('error fetching summary', err);
    }
  );
}

export function getWorkspaceList(workspaceId) {
  const namespace = NamespaceStore.getState().selectedNamespace;
  const params = {
    context: namespace,
  };

  MyDataPrepApi.getWorkspaceList(params).subscribe((res) => {
    if (res.values.length === 0) {
      DataPrepStore.dispatch({
        type: DataPrepActions.setWorkspaceList,
        payload: {
          list: [],
        },
      });

      return;
    }

    let workspaceList = orderBy(
      res.values,
      [(workspace) => (workspace.name || '').toLowerCase()],
      ['asc']
    );

    DataPrepStore.dispatch({
      type: DataPrepActions.setWorkspaceList,
      payload: {
        list: workspaceList,
      },
    });

    if (workspaceId) {
      // Set active workspace
      // Check for existance of the workspaceId
      let workspaceObj = find(workspaceList, { id: workspaceId });

      let workspaceObservable$;
      if (workspaceObj) {
        workspaceObservable$ = setWorkspace(workspaceId);
      } else {
        workspaceObservable$ = setWorkspace(workspaceList[0].id);
      }

      workspaceObservable$.subscribe();
    }
  });
}

export function setVisualizationState(state) {
  DataPrepStore.dispatch({
    type: DataPrepActions.setProperties,
    payload: {
      properties: {
        visualization: state,
      },
    },
  });
}

export function setError(error, prefix) {
  const status = error.statusCode;
  const detail = error.message || error.response.message || 'Unknown error';
  const message = `${prefix || 'Error'}: ${status ? `${status}: ${detail}` : detail}`;
  DataPrepStore.dispatch({
    type: DataPrepActions.setError,
    payload: {
      message,
    },
  });
}

export async function loadTargetDataModelStates() {
  // These properties were populated by MyDataPrepApi.getWorkspace API
  const {
    dataModel,
    dataModelRevision,
    dataModelModel,
  } = DataPrepStore.getState().dataprep.properties;

  let { dataModelList } = DataPrepStore.getState().dataprep;
  if (!Array.isArray(dataModelList)) {
    dataModelList = await fetchDataModelList(Theme.wranglerDataModelUrl);
  }

  const rev = Number(dataModelRevision);
  const targetDataModel = dataModelList.find((dm) => dm.id === dataModel && dm.revision === rev);
  await setTargetDataModel(targetDataModel);
  if (targetDataModel) {
    await setTargetModel(targetDataModel.models.find((m) => m.id === dataModelModel));
  } else {
    await setTargetModel(null);
  }
}

export async function saveTargetDataModelStates() {
  const params = {
    context: NamespaceStore.getState().selectedNamespace,
    workspaceId: DataPrepStore.getState().dataprep.workspaceId,
  };

  const { targetDataModel, targetModel } = DataPrepStore.getState().dataprep;
  const newDataModelId = targetDataModel ? targetDataModel.id : null;
  const newDataModelRevision = targetDataModel ? targetDataModel.revision : null;
  const newModelId = targetModel ? targetModel.id : null;

  // These properties were populated by MyDataPrepApi.getWorkspace API
  const {
    dataModel,
    dataModelRevision,
    dataModelModel,
  } = DataPrepStore.getState().dataprep.properties;
  const oldDataModelId = dataModel || null;
  const oldDataModelRevision = isFinite(dataModelRevision) ? Number(dataModelRevision) : null;
  const oldModelId = dataModelModel || null;

  if (oldDataModelId !== newDataModelId || oldDataModelRevision !== newDataModelRevision) {
    if (oldDataModelId !== null) {
      await MyDataPrepApi.detachDataModel(params).toPromise();
    }
    if (newDataModelId !== null) {
      await MyDataPrepApi.attachDataModel(params, {
        id: newDataModelId,
        revision: newDataModelRevision,
      }).toPromise();
    }
  }

  if (oldModelId !== newModelId) {
    if (oldModelId !== null) {
      await MyDataPrepApi.detachModel(
        Object.assign(
          {
            modelId: oldModelId,
          },
          params
        )
      ).toPromise();
    }
    if (newModelId !== null) {
      await MyDataPrepApi.attachModel(params, {
        id: newModelId,
      }).toPromise();
    }
  }

  DataPrepStore.dispatch({
    type: DataPrepActions.setProperties,
    payload: {
      properties: {
        dataModel: newDataModelId,
        dataModelRevision: newDataModelRevision,
        dataModelModel: newModelId,
      },
    },
  });
}

export async function fetchDataModelList(url) {
  const namespace = NamespaceStore.getState().selectedNamespace;
  const params = {
    context: namespace,
  };

  await MyDataPrepApi.addDataModels(params, { url }).toPromise();
  const response = await MyDataPrepApi.getDataModels(params).toPromise();
  const dataModelList = response.values.map((dataModel) => ({
    id: dataModel['namespacedId'].id,
    revision: dataModel.revision,
    name: dataModel.displayName,
    description: dataModel.description,
    url,
  }));
  dataModelList.sort((a, b) => a.name.localeCompare(b.name));

  DataPrepStore.dispatch({
    type: DataPrepActions.setDataModelList,
    payload: {
      dataModelList,
    },
  });

  return dataModelList;
}

export async function fetchModelList(dataModel) {
  const params = {
    context: NamespaceStore.getState().selectedNamespace,
    dataModelId: dataModel.id,
    dataModelRevision: dataModel.revision,
  };

  const response = await MyDataPrepApi.getDataModel(params).toPromise();

  try {
    const data = JSON.parse(response.message || null);
    if (data && Array.isArray(data.fields)) {
      dataModel.models = [];
      data.fields.forEach((entry) => {
        if (Array.isArray(entry.type)) {
          const model = entry.type.find((record) => typeof record === 'object');
          if (model && typeof model.name === 'string') {
            if ((model.name = model.name.trim()).length > 0) {
              const fields = [];
              if (Array.isArray(model.fields)) {
                model.fields.forEach((field) => {
                  if (field && typeof field.name === 'string') {
                    if ((field.name = field.name.trim()).length > 0) {
                      fields.push({
                        id: field.name,
                        name: field.name,
                        description: field.doc,
                      });
                    }
                  }
                });
                fields.sort((a, b) => a.name.localeCompare(b.name));
              }
              dataModel.models.push({
                id: model.name,
                name: model.name,
                description: model.doc,
                fields,
              });
            }
          }
        }
      });
      dataModel.models.sort((a, b) => a.name.localeCompare(b.name));
    }
  } catch (error) {
    throw new Error(
      `Malformed definition received for "${dataModel.id}" data model of ${dataModel.revision} revision: ${error}`
    );
  }

  return dataModel.models;
}

export async function setTargetDataModel(dataModel) {
  if (dataModel && !Array.isArray(dataModel.models)) {
    await fetchModelList(dataModel);
  }

  DataPrepStore.dispatch({
    type: DataPrepActions.setTargetDataModel,
    payload: {
      targetDataModel: dataModel || null,
    },
  });
}

export function setTargetModel(model) {
  DataPrepStore.dispatch({
    type: DataPrepActions.setTargetModel,
    payload: {
      targetModel: model || null,
    },
  });
}
