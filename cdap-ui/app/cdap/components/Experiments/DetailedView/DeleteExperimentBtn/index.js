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
import {myExperimentsApi} from 'api/experiments';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {setExperimentDetailError} from 'components/Experiments/store/ExperimentDetailActionCreator';

const deleteExperiment = (experimentId, callback, errCallback) => {
  let namespace = getCurrentNamespace();
  myExperimentsApi
    .deleteExperiment({
      namespace,
      experimentId
    })
    .subscribe(
      () => window.location.href =`${window.location.origin}/cdap/ns/${namespace}/experiments`,
      err => {
        let error = `Failed to delete experiment '${experimentId}': ${err.response || err}`;
        errCallback(error);
      }
    );
};

const deleteConfirmElement = (experimentId) => <div>Are you sure you want to delete <b>{experimentId}</b> experiment </div>;

export default function DeleteExperimentBtn({experimentId}) {
  return (
    <DeleteEntityBtn
      confirmFn={deleteExperiment.bind(null, experimentId, null, setExperimentDetailError)}
      className="btn btn-link"
      headerTitle={"Delete Model"}
      confirmationElem={deleteConfirmElement(experimentId)}
      btnLabel={"Delete Experiment"}
    />
  );
}

DeleteExperimentBtn.propTypes = {
  experimentId: PropTypes.string
};
