/*
 * Copyright © 2016 Cask Data, Inc.
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

import {combineReducers, createStore} from 'redux';
const defaultAction = {
  type: '',
  payload: {}
};

const defaultState = {
  name: 'etlSchemabody',
  type: 'record',
  fields: [
    {
      name: '',
      type: {},
      displayType: 'string'
    }
  ]
};

const schema = (state = defaultState, action = defaultAction) => {
  switch(action.type) {
    case 'FIELD_UPDATE': {
      return Object.assign({}, state, {fields: action.payload.schema.fields});
    }
    default:
      return state;
  }
};

var initialFields = [
    {
        "name": "email",
        "type": "string"
    },
    {
        "name": "phone",
        "type": "int"
    },
    {
        "name": "age",
        "type": "long"
    },
    {
        "name": "addresses",
        "type": {
            "type": "array",
            "items": "string"
        }
    },
    {
        "name": "userid",
        "type": {
            "type": "record",
            "name": "a2b2e0a4c40984f5f852fc9ca3546c325",
            "fields": [
                {
                    "name": "FK_USERID",
                    "type": "string"
                },
                {
                    "name": "FK_EMAIL",
                    "type": "string"
                }
            ]
        }
    },
    {
        "name": "userpref",
        "type": {
            "type": "enum",
            "symbols": [
                "NOEMAIL",
                "SOMEEMAIL",
                "PROMOEMAIL"
            ]
        }
    },
    {
        "name": "FK1",
        "type": {
            "type": "map",
            "keys": {
                "type": "map",
                "keys": {
                    "type": "array",
                    "items": "string"
                },
                "values": {
                    "type": "enum",
                    "symbols": [
                        "EANUM1"
                    ]
                }
            },
            "values": "string"
        }
    },
    {
        "name": "SOmeKey",
        "type": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "map",
                    "keys": "string",
                    "values": {
                        "type": "record",
                        "name": "a4c29a28e180043f49d41d3eff679506b",
                        "fields": [
                            {
                                "name": "username",
                                "type": "string"
                            },
                            {
                                "name": "password",
                                "type": "string"
                            }
                        ]
                    }
                }
            }
        }
    }
];

var initialState = {
  "schema": {
    "name": "etlSchemabody",
    "type": "record",
    "fields": initialFields
  }
};

let createStoreInstance = () => {
  return createStore(
    combineReducers({
      schema
    }),
    initialState,
    window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
  );
};

export {createStoreInstance};
let SchemaStore =  createStoreInstance();
export default SchemaStore;
