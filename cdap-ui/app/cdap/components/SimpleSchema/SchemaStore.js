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
import SchemaStoreActions from './SchemaStoreActions';
const defaultRow = {
 name: '',
 type: '',
 isNullable: false
};
const defaultSchema = {
 "type":"record",
 "name":"etlSchemaBody",
 "fields":[defaultRow]
};
const defaultAction = {
 type: '',
 payload: {}
};
const schema = (state = defaultSchema, action = defaultAction) => {
 let stateCopy;
 switch (action.type) {
   case SchemaStoreActions.setFieldName:
     stateCopy = Object.assign({}, state);
     if (action.payload.keyCode === 13) {
       stateCopy.fields.push({
         name: '',
         type: '',
         isNullable: false
       });
       return stateCopy;
     }
     stateCopy = Object.assign({}, state);
     if (action.payload.name === null || typeof action.payload.name === 'undefined') {
       return stateCopy;
     }
     stateCopy.fields[action.payload.index].name = action.payload.name;
     return stateCopy;
   case SchemaStoreActions.setFieldType:
     stateCopy = Object.assign({}, state);
     stateCopy.fields[action.payload.index].type = action.payload.type;
     return stateCopy;
   case SchemaStoreActions.setFieldIsNullable:
     stateCopy = Object.assign({}, state);
     stateCopy.fields[action.payload.index].isNullable = action.payload.isNullable;
     return stateCopy;
   case SchemaStoreActions.removeField:
      if (action.payload.keyCode === 13) {
         return state;
      }
     stateCopy = Object.assign({}, state);
     stateCopy.fields.splice(action.payload.index, 1);
     if (!stateCopy.fields.length) {
        stateCopy.fields.push({
         name: '',
         type: '',
         isNullable: false
       });
     }
     return stateCopy;
   default:
     return state;
 }
};

const createSchemaStore = (initialState = defaultSchema) => {
  return createStore(
    combineReducers({schema}),
    initialState
  );
};

export { createSchemaStore };
