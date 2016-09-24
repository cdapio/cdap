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
import PublishPipelineAction from 'services/WizardStores/PublishPipeline/PublishPipelineActions';
import PublishPipelineWizardConfig from 'services/WizardConfigs/PublishPipelineWizardConfig';
import head from 'lodash/head';

const defaultAction = {
  type: '',
  payload: {}
};
const defaultMetadata = {
  name: '',
  pipelineConfig: null,
  __complete: false,
  __skipped: false,
  __error: false
};
const defaultInitialState = {
  pipelinemetadata: defaultMetadata
};
const isNil = (value) => value === null || typeof value === 'undefined' || value === '';
const isComplete = (state, requiredFields) => {
  let emptyFieldsInState = Object.keys(state)
    .filter(fieldName => {
      return isNil(state[fieldName]) && requiredFields.indexOf(fieldName) !== -1;
    });
  return !emptyFieldsInState.length ? true : false;
};
const pipelineMetadataStepRequiredFields = head(
  PublishPipelineWizardConfig
    .steps
    .filter(step => step.id === 'pipelinemetadata')
  ).requiredFields;

const pipelinemetadata = (state = defaultMetadata, action = defaultAction) => {
  let stateCopy;
  switch(action.type) {
    case PublishPipelineAction.setPipelineName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name
      });
      break;
    case PublishPipelineAction.setPipelineConfig:
      stateCopy = Object.assign({}, state, {
        pipelineConfig: action.payload.pipelineConfig
      });
      break;
    case PublishPipelineAction.onReset:
      return defaultMetadata;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, pipelineMetadataStepRequiredFields)
  });
};

const publishPipelineStoreWrapper = () => {
  return createStore(
    combineReducers({
      pipelinemetadata,
    }),
    defaultInitialState
  );
};

const PublishPipelineStore = publishPipelineStoreWrapper();
export default PublishPipelineStore;
