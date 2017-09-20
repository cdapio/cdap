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
import findIndex from 'lodash/findIndex';
import T from 'i18n-react';

const SCHEDULERUNTIMEARGSACTIONS = {
  SETTRIGGERINGPIPELINEINFO: 'SETMACROSANDCONFIGSTAGES',
  SETTRIGGEREDPIPELINEINFO: 'SETTRIGGEREDPIPELINEINFO',
  SETCONFIGSTAGES: 'SETCONFIGSTAGES',
  SETARGSKEY: 'SETARGSKEY',
  SETARGSVALUE: 'SETARGSVALUE',
  BULKSETARGSVALUE: 'BULKSETARGSVALUE',
  SETDISABLED: 'SETDISABLED',
  RESET: 'RESET'
};

const I18NPREFIX = 'features.PipelineTriggers.ScheduleRuntimeArgs';

const DEFAULTRUNTIMEARGSMESSAGE = T.translate(`${I18NPREFIX}.DefaultMessages.choose_runtime_arg`);
const DEFAULTSTAGEMESSAGE = T.translate(`${I18NPREFIX}.DefaultMessages.choose_plugin`);
const DEFAULTPROPERTYMESSAGE = T.translate(`${I18NPREFIX}.DefaultMessages.choose_plugin_property`);
const DEFAULTTRIGGEREDMACROMESSAGE = T.translate(`${I18NPREFIX}.DefaultMessages.choose_runtime_arg_map`);
const DEFAULTFIELDDELIMITER = ':';

const DEFAULTMACROS = [];
const DEFAULTCONFIGSTAGES = [];
const DEFAULTARGSMAPPING = [];
const UNMAPPEDCONFIGSTAGEPROPERTIES = {};
const DEFAULTARGS = {
  triggeringPipelineInfo: {
    macros: DEFAULTMACROS,
    configStages: DEFAULTCONFIGSTAGES,
    configStagesMap: UNMAPPEDCONFIGSTAGEPROPERTIES,
    id: null
  },
  triggeredPipelineInfo: {
    macros: DEFAULTMACROS,
    unMappedMacros: DEFAULTMACROS,
    id: null
  },
  argsMapping: DEFAULTARGSMAPPING,
  disabled: false
};

const isDefaultMessage = (input) => [DEFAULTRUNTIMEARGSMESSAGE, DEFAULTSTAGEMESSAGE, DEFAULTPROPERTYMESSAGE, DEFAULTTRIGGEREDMACROMESSAGE].indexOf(input) !== -1;

const handleDefaultMessage = (input) => {
  if (typeof input === 'undefined') {
    return '';
  }
  return  isDefaultMessage(input) ? '' : input;
};

const arrayToMap = (stages) => {
  let map = {};
  stages.forEach(stage => {
    let properties = stage.properties;

    map[stage.id] = {
      properties
    };
  });
  return map;
};

const getUpdatedMapping = (state, {key, value, type}, oldValue) => {
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
      value: handleDefaultMessage(value),
      type
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
          configStages: action.payload.configStages,
          configStagesMap: arrayToMap(action.payload.configStages),
          id: action.payload.id
        }
      });
    case SCHEDULERUNTIMEARGSACTIONS.SETTRIGGEREDPIPELINEINFO:
      return Object.assign({}, state, {
        triggeredPipelineInfo: {
          macros: action.payload.macros,
          unMappedMacros: action.payload.macros,
          id: action.payload.id
        }
      });
    case SCHEDULERUNTIMEARGSACTIONS.SETARGSVALUE: {
      let argsMapping = getUpdatedMapping(state, {
        key: action.payload.mappingKey,
        value: action.payload.mappingValue,
        type: action.payload.type
      }, action.payload.oldMappedValue);

      let returnObj = Object.assign({}, state, {
        argsMapping,
        triggeredPipelineInfo: Object.assign({}, state.triggeredPipelineInfo, {
          unMappedMacros: action.payload.mappingKey && action.payload.mappingValue ?
            difference(state.triggeredPipelineInfo.macros, argsMapping.map(arg => arg.value))
            :
            state.triggeredPipelineInfo.unMappedMacros
        })
      });

      return returnObj;
    }
    case SCHEDULERUNTIMEARGSACTIONS.BULKSETARGSVALUE:
      return {
        ...state,
        argsMapping: action.payload.argsArray
      };
    case SCHEDULERUNTIMEARGSACTIONS.SETDISABLED:
      return Object.assign({}, state, {
        disabled: true
      });
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
