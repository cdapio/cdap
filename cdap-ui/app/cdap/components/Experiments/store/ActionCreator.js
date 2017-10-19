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

import experimentsStore, {ACTIONS} from 'components/Experiments/store';
import {myExperimentsApi} from 'api/experiments';
import NamespaceStore from 'services/NamespaceStore';

function setExperimentsLoading() {
  experimentsStore.dispatch({
    type: ACTIONS.SET_EXPERIMENTS_LOADING
  });
}

function getExperimentsList() {
  setExperimentsLoading();
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .list({namespace})
    .subscribe(experiments => {
      experiments.forEach(experiment => getModelsListInExperiment(experiment.name));
      experimentsStore.dispatch({
        type: ACTIONS.SET_EXPERIMENTS_LIST,
        payload: {
          experiments
        }
      });
    });
}

function getModelsListInExperiment(experimentId) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .getModelsInExperiment({experimentId, namespace})
    .subscribe(models => {
      experimentsStore.dispatch({
        type: ACTIONS.SET_MODELS_IN_EXPERIMENT,
        payload: {
          experimentId,
          models
        }
      });
    });
}

export {
  setExperimentsLoading,
  getExperimentsList,
  getModelsListInExperiment
};

