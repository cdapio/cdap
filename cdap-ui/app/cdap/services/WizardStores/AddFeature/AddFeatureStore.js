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

import { combineReducers, createStore } from 'redux';
import AddFeatureActions from 'services/WizardStores/AddFeature/AddFeatureActions';
import isEmpty from 'lodash/isEmpty';
import remove from 'lodash/remove';
import find from 'lodash/find';
import findIndex from 'lodash/findIndex';


const defaultAction = {
  type: '',
  payload: {}
};
const defaultState = {
  steps: [],
  operationType: 'CREATE_PIPELINE',
  featureName: '',
  availableSchemas: [],
  availableProperties: [],
  availableConfigurations: [],
  selectedSchemas: [],
  detectedProperties: [],
  propertyMap: new Map(),
  configurationList: [],
  __complete: false,
  __skipped: false,
  __error: false
};
const defaultInitialState = {
  featureState: defaultState,
};


const isFeatureComplete = (state) => {
  if (isEmpty(state.featureName)) {
    return false;
  }
  if (isEmpty(state.selectedSchemas)) {
    return false;
  }

  let mandatoryProperties = state.availableProperties.filter(item => item.isMandatory == true).map(item => item.paramName);

  let mandatoryPropsSet = 0;
  state.propertyMap.forEach((value, key) => {
    if (mandatoryProperties.indexOf(key) >= 0) {
      for (let i = 0; i < value.length; i++) {
        let size = state.propertyMap.get(key).size;
        if (size == 0) {
          return false;
        } else {
          mandatoryPropsSet++;
        }
      }
    } else {
      let property = find(state.availableProperties, { paramName: key });
      if (property && !isEmpty(property.subParams)) {
        if (property.subParams.length != value.length) {
          return false;
        }
      }
    }
  });

  if (mandatoryPropsSet < mandatoryProperties.length) {
    return false;
  }

  if (isEmpty(state.configurationList)) {
    return false;
  } else {
    for (let i = 0; i < state.availableConfigurations.length; i++) {
      if (state.availableConfigurations[i].isMandatory) {
        let configuredProperty = find(state.configurationList, { name: state.availableConfigurations[i].paramName });
        if (configuredProperty) {
          if (isEmpty(configuredProperty.value)) {
            return false;
          }
        } else {
          return false;
        }
      }
    }
  }
  return true;
};


const featureState = (state = defaultState, action = defaultAction) => {
  switch (action.type) {
    case AddFeatureActions.saveFeature:
      break;
    case AddFeatureActions.onReset:
      state = {
        ...defaultState,
        propertyMap: new Map()
      };
      break;
    case AddFeatureActions.updateOperationType:
      state = {
        ...state,
        operationType: action.payload
      };
      break;
    case AddFeatureActions.updateFeatureName:
      state = {
        ...state,
        featureName: action.payload
      };
      break;
    case AddFeatureActions.setAvailableProperties:
      state = {
        ...state,
        availableProperties: action.payload
      };
      break;
    case AddFeatureActions.setAvailableConfigurations:
      state = {
        ...state,
        availableConfigurations: action.payload
      };
      break;
    case AddFeatureActions.setAvailableSchemas:
      state = {
        ...state,
        availableSchemas: action.payload
      };
      break;
    case AddFeatureActions.setSelectedSchemas:
      state = {
        ...state,
        selectedSchemas: action.payload
      };
      break;
    case AddFeatureActions.addSelectedSchema:
      state = {
        ...state,
        selectedSchemas: [...state.selectedSchemas, action.payload]
      };
      break;
    case AddFeatureActions.updateSelectedSchema:
      {
        let selectedSchemas = state.selectedSchemas;
        let index = findIndex(selectedSchemas, { schemaName: action.payload.schemaName });
        if (index >= 0) {
          selectedSchemas[index] = action.payload;
          state = {
            ...state,
            selectedSchemas: selectedSchemas
          };
        }
      }
      break;
    case AddFeatureActions.deleteSelectedSchema: {
      let selectedSchemas = state.selectedSchemas;
      remove(selectedSchemas, { schemaName: action.payload.schemaName });
      state = {
        ...state,
        selectedSchemas: selectedSchemas
      };
    }
      break;
    case AddFeatureActions.updatePropertyMap: {
      state = {
        ...state,
        propertyMap: action.payload
      };
    }
      break;
    case AddFeatureActions.updateConfigurationList: {
      state = {
        ...state,
        configurationList: action.payload
      };
    }
      break;
    case AddFeatureActions.setDetectedProperties:
      state = {
        ...state,
        detectedProperties: action.payload
      };
      break;
  }
  return {
    ...state,
    __complete: isFeatureComplete(state)
  };
};


const AddFeatureStore = createStore(
  combineReducers({
    featureState,
  }),
  defaultInitialState
);

export default AddFeatureStore;
