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

import {createStore, combineReducers} from 'redux';
import {defaultAction} from 'services/helpers';
import difference from 'lodash/difference';
import isNil from 'lodash/isNil';
import findIndex from 'lodash/findIndex';

const SCHEDULERUNTIMEARGSACTIONS = {
  SETTRIGGERINGPIPELINEINFO: 'SETMACROSANDCONFIGSTAGES',
  SETTRIGGEREDPIPELINEINFO: 'SETTRIGGEREDPIPELINEINFO',
  SETCONFIGSTAGES: 'SETCONFIGSTAGES',
  SETARGSKEY: 'SETARGSKEY',
  SETARGSVALUE: 'SETARGSVALUE',
  RESET: 'RESET'
};

const DEFAULTRUNTIMEARGSMESSAGE = 'Choose a Runtime Argument';
const DEFAULTSTAGEMESSAGE = 'Choose a Plugin';
const DEFAULTPROPERTYMESSAGE = 'Choose a Plugin Property';
const DEFAULTTRIGGEREDMACROMESSAGE = 'Choose a Runtime Argument to map';
const DEFAULTFIELDDELIMITER = ':';

const DEFAULTMACROS = [];
const DEFAULTCONFIGSTAGES = [];
const DEFAULTARGSMAPPING = [];
const UNMAPPEDCONFIGSTAGEPROPERTIES = {};
const DEFAULTARGS = {
  triggeringPipelineInfo: {
    macros: DEFAULTMACROS,
    unMappedMacros: DEFAULTMACROS,
    configStages: DEFAULTCONFIGSTAGES,
    unMappedConfigStages: DEFAULTCONFIGSTAGES,
    unMappedConfigStageProperties: UNMAPPEDCONFIGSTAGEPROPERTIES,
    id: null
  },
  triggeredPipelineInfo: {
    macros: DEFAULTMACROS,
    unMappedMacros: DEFAULTMACROS,
    id: null
  },
  argsMapping: DEFAULTARGSMAPPING
};

const isDefaultMessage = (input) => [DEFAULTRUNTIMEARGSMESSAGE, DEFAULTSTAGEMESSAGE, DEFAULTPROPERTYMESSAGE, DEFAULTTRIGGEREDMACROMESSAGE].indexOf(input) !== -1;

const handleDefaultMessage = (input) => {
  if (typeof input === 'undefined') {
    return '';
  }
  return  isDefaultMessage(input) ? '' : input;
};

const getUsedProperties = (mapping) => {
  let usedProperties = {};
  mapping.forEach(arg => {
    let key = arg.key;
    if (isNil(key)) {
      return;
    }
    key = key.split(DEFAULTFIELDDELIMITER);
    if (key.length === 1) {
      return;
    }
    let stage = key[1];
    let property = key[2];
    if (isNil(usedProperties[stage])) {
      usedProperties[stage] = [property];
    } else {
      usedProperties[stage].push(property);
    }
  });
  return usedProperties;
};

const getUnusedProperties = (usedProperties, stages) => {
  let unUsedProperties = {};
  stages.forEach(stage => {
    let usedPropertiesForStage = usedProperties[stage.id] || [];
    let properties = stage.properties;
    if (usedPropertiesForStage.length) {
      // filter out used properties
      properties = properties.filter(prop => usedPropertiesForStage.indexOf(prop) === -1);
    }
    unUsedProperties[stage.id] = {
      properties
    };
  });
  return unUsedProperties;
};

const getUpdatedMapping = (state, {key, value}, oldValue) => {
  let argsMapping = [...state.argsMapping];
  let existingMap = {};

  // This means one macro is changed as something else. Remove the existing map. That becomes invalid now.
  if (oldValue !== value) {
    argsMapping = argsMapping.filter(arg => arg.value !== oldValue);
  }

  if (isDefaultMessage(value) || isDefaultMessage(key) || !value || !key) {
    argsMapping = argsMapping.filter(arg => arg.value !== oldValue);
    return argsMapping;
  }
  // This means we have a valid key and value. Either update or insert the new one.
  existingMap = findIndex(argsMapping, arg => arg.value === value);
  if (existingMap !== -1) {
    argsMapping[existingMap].key = key;
  } else {
    existingMap = {
      key: handleDefaultMessage(key),
      value: handleDefaultMessage(value)
    };
    argsMapping.push(existingMap);
  }
  return argsMapping;
};

const args = (state = DEFAULTARGS, action = defaultAction) => {
  switch (action.type) {
    case SCHEDULERUNTIMEARGSACTIONS.SETTRIGGERINGPIPELINEINFO:
      return Object.assign({}, state, {
        triggeringPipelineInfo: {
          macros: action.payload.macros,
          unMappedMacros: action.payload.macros,
          configStages: action.payload.configStages,
          unMappedConfigStages: action.payload.configStages,
          unMappedConfigStageProperties: getUnusedProperties({}, action.payload.configStages),
          id: action.payload.id
        }
      });
    case SCHEDULERUNTIMEARGSACTIONS.SETTRIGGEREDPIPELINEINFO:
      return Object.assign({}, state, {
        triggeredPipelineInfo: {
          macros: action.payload.macros,
          unMappedMacros: action.payload.macros,
          id: action.payload.id
        },
        argsMapping: []
      });
    case SCHEDULERUNTIMEARGSACTIONS.SETARGSVALUE: {
      let argsMapping = getUpdatedMapping(state, {
        key: action.payload.mappingKey,
        value: action.payload.mappingValue
      }, action.payload.oldMappedValue);
      let usedProperties = getUsedProperties(argsMapping);
      let unUsedProperties = getUnusedProperties(usedProperties, state.triggeringPipelineInfo.configStages);
      return Object.assign({}, state, {
        argsMapping,
        triggeredPipelineInfo: Object.assign({}, state.triggeredPipelineInfo, {
          unMappedMacros: action.payload.mappingKey && action.payload.mappingValue ?
            difference(state.triggeredPipelineInfo.macros, argsMapping.map(arg => arg.value))
            :
            state.triggeredPipelineInfo.macros
        }),
        triggeringPipelineInfo: Object.assign({}, state.triggeringPipelineInfo, {
          unMappedConfigStageProperties: unUsedProperties,
          unMappedMacros: difference(state.triggeringPipelineInfo.macros, argsMapping.map(arg => arg.key))
        })
      });
    }
    case SCHEDULERUNTIMEARGSACTIONS.RESET:
      return DEFAULTARGS;
    default:
      return state;
  }
};

let ScheduleRuntimeArgsStore = createStore(
  combineReducers({
    args
  }),
  {
    args: DEFAULTARGS
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);
export default ScheduleRuntimeArgsStore;
export {
  SCHEDULERUNTIMEARGSACTIONS,
  DEFAULTRUNTIMEARGSMESSAGE,
  DEFAULTSTAGEMESSAGE,
  DEFAULTPROPERTYMESSAGE,
  DEFAULTTRIGGEREDMACROMESSAGE,
  DEFAULTFIELDDELIMITER
};
