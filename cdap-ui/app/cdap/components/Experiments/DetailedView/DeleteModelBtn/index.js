/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';
import React from 'react';
import DeleteEntityBtn from 'components/DeleteEntityBtn';
import {
  getModelsInExperiment,
  setExperimentDetailError,
} from 'components/Experiments/store/ExperimentDetailActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import { myExperimentsApi } from 'api/experiments';

const deleteModel = (experimentId, model, callback, errCallback) => {
  let { selectedNamespace: namespace } = NamespaceStore.getState();
  myExperimentsApi
    .deleteModelInExperiment({
      namespace,
      experimentId,
      modelId: model.id,
    })
    .subscribe(
      () => {
        getModelsInExperiment(experimentId);
        callback();
      },
      (err) => {
        let error = `Failed to delete the model '${model.name}' - ${err.response || err}`;
        errCallback(error);
      }
    );
};

const deleteConfirmElement = (model) => (
  <div>
    Are you sure you want to delete the model <b>{model.name}</b>?
  </div>
);

export default function DeleteModelBtn({ experimentId, model }) {
  return (
    <DeleteEntityBtn
      confirmFn={deleteModel.bind(null, experimentId, model, null, setExperimentDetailError)}
      headerTitle={'Delete Model'}
      confirmationElem={deleteConfirmElement(model)}
    />
  );
}

DeleteModelBtn.propTypes = {
  experimentId: PropTypes.string,
  model: PropTypes.shape({
    id: PropTypes.string,
    name: PropTypes.string,
  }),
};
