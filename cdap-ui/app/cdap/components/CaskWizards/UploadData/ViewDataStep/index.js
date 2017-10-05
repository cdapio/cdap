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
import PropTypes from 'prop-types';

import React from 'react';
import { connect, Provider } from 'react-redux';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
import fileDownload from 'react-file-download';
require('./ViewDataStep.scss');

const mapStateWithProps = (state) => {
  return {
    value: state.viewdata.data,
    isLoading: state.viewdata.loading
  };
};

const handleDownload = () => {
  const state = UploadDataStore.getState();

  fileDownload(state.viewdata.data, state.viewdata.filename);
};

let DataTextArea = ({value, isLoading}) => {
  value = value.substring(0, 10000);
  return (
    <div className="datapack-container">
      <div className="view-data-step">
        <div className="download-section text-xs-right">
          <button
            className="btn btn-link"
            onClick={handleDownload}
          >
            <span className="fa fa-download"></span>
          </button>
        </div>
        {
          isLoading ?
            <div className="loading text-xs-center"><i className="fa fa-spinner fa-spin" /></div>
            :
            <pre>
              {value}
            </pre>
        }
      </div>
    </div>
  );
};

DataTextArea.propTypes = {
  value: PropTypes.string,
  isLoading: PropTypes.bool
};

DataTextArea = connect(
  mapStateWithProps,
  null
)(DataTextArea);

export default function ViewDataStep() {
  let { filename, packagename, packageversion, data } = UploadDataStore.getState().viewdata;

  if (!data) {
    UploadDataActionCreator
      .fetchDefaultData({ filename, packagename, packageversion });
  }

  return (
    <Provider store={UploadDataStore}>
      <DataTextArea />
    </Provider>
  );
}
