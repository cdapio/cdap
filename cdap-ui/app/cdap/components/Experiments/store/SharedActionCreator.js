/*
 * Copyright © 2018 Cask Data, Inc.
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

import { myExperimentsApi } from 'api/experiments';
import { getCurrentNamespace } from 'services/NamespaceStore';
import AlgorithmsListStore, {
  ACTIONS as AlgorithmsStoreActions,
} from 'components/Experiments/store/AlgorithmsListStore';
import { Observable } from 'rxjs/Observable';

const getAlgorithmLabel = (algorithm) => {
  let algorithmsList = AlgorithmsListStore.getState();
  let match = algorithmsList.find((algo) => algo.name === algorithm);
  if (match) {
    return match.label;
  }
  return algorithm;
};

const getHyperParamLabel = (algorithm, hyperparam) => {
  let algorithmsList = AlgorithmsListStore.getState();
  let match = algorithmsList.find((algo) => algo.name === algorithm);
  if (match) {
    let matchingHyperParameter = match.hyperparameters.find((hp) => hp.name === hyperparam);
    if (matchingHyperParameter) {
      return matchingHyperParameter.label;
    }
  }
  return hyperparam;
};

const setAlgorithmsList = () => {
  let algoList = AlgorithmsListStore.getState();
  if (algoList.length) {
    return Observable.of();
  }

  const getAlgorithmsApi = myExperimentsApi.getAlgorithms({
    namespace: getCurrentNamespace(),
  });

  getAlgorithmsApi.subscribe((algorithmsList) => {
    algorithmsList = algorithmsList.map((algo) => ({ ...algo, name: algo.algorithm }));
    AlgorithmsListStore.dispatch({
      type: AlgorithmsStoreActions.SET_ALGORITHMS_LIST,
      payload: { algorithmsList },
    });
  });

  return getAlgorithmsApi;
};

export { getAlgorithmLabel, getHyperParamLabel, setAlgorithmsList };
