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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import {connect, Provider} from 'react-redux';
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import {createSplitAndUpdateStatus, setSplitFinalized, setModelCreateError} from 'components/Experiments/store/CreateExperimentActionCreator';
import {getCurrentNamespace} from 'services/NamespaceStore';
import SplitInfo from 'components/Experiments/CreateView/SplitDataStep/SplitInfo';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import isEmpty from 'lodash/isEmpty';

require('./SplitDataStep.scss');
const getSplitLogsUrl = (experimentId, splitInfo) => {
  let splitId = splitInfo.id;
  let {routerServerUrl, routerServerPort} = window.CDAP_CONFIG.cdap;
  let protocol = window.CDAP_CONFIG.sslEnabled ? 'https' : 'http';
  if (routerServerUrl === '127.0.0.1') {
    routerServerUrl = 'localhost';
  }
  let hostPort = `${protocol}://${routerServerUrl}:${routerServerPort}`;
  let baseUrl = `/v3/namespaces/${getCurrentNamespace()}/apps/ModelManagementApp/spark/ModelManagerService/logs`;
  let queryParams = encodeURI(`?filter=MDC:experiment="${experimentId}" AND MDC:split=${splitId}`);
  return `${hostPort}${baseUrl}${queryParams}`;
};

const getSplitFailedElem = (experimentId, splitInfo) => {
  return (
    <span className="split-error-container">
      {`Failed to split data for the experiment '${experimentId}' - Please check `}
      <a href={getSplitLogsUrl(experimentId, splitInfo)} target="_blank"> Logs </a>{" "}
      for more information
    </span>
  );
};

class SplitDataStep extends Component {
  state = {
    splitFailed: false
  };

  static propTypes = {
    splitInfo: PropTypes.object,
    experimentId: PropTypes.string,
    error: PropTypes.string
  };

  componentWillReceiveProps(nextProps) {
    const isValidSplitStatus = this.props.splitInfo.status;
    const wasSplitting = ['CREATING', 'Splitting'].indexOf(this.props.splitInfo.status) !== -1;
    const isCurrentSplitStatusFailed = nextProps.splitInfo.status === 'Failed';

    if (isValidSplitStatus && wasSplitting && isCurrentSplitStatusFailed) {
      this.setState({
        splitFailed: true
      });
    }
  }

  closeSplitFailedAlert = () => {
    this.setState({
      splitFailed: false
    });
  }

  renderSplitBtn() {
    let isSplitCreated = Object.keys(this.props.splitInfo).length;
    let splitStatus = (this.props.splitInfo || {}).status;
    let isSplitComplete = ['Complete', 'Failed'].indexOf(splitStatus) !== -1;

    if (!isSplitCreated || (isSplitCreated && isSplitComplete)) {
      return (
        <div>
          <button
            className="btn btn-primary"
            onClick={createSplitAndUpdateStatus}
          >
            Split data and verify sample
          </button>
        </div>
      );
    }
    return (
      <button
        className="btn btn-primary"
        disabled
      >
        <div className="btn-inner-container">
          <IconSVG name="icon-spinner" className="fa-spin" />
          <span>Splitting data</span>
        </div>
      </button>
    );
  }

  renderSplitInfo() {
    const splitStatus = this.props.splitInfo.status || 'Complete';

    if (isEmpty(this.props.splitInfo) || splitStatus !== 'Complete') {
      return null;
    }

    return (
      <div className="action-button-group">
        <SplitInfo />
        <button
          className="btn btn-primary"
          onClick={setSplitFinalized}
          disabled={splitStatus !== 'Complete'}
        >
          Done
        </button>
        <span> Next, Select a machine learning algorithm </span>
      </div>
    );
  }

  renderError() {
    if (!this.props.error && !this.state.splitFailed) {
      return null;
    }

    /*
      We have 2 cases where we want to show an Alert on this page:
      1. When the split status request returns 200, but status is 'Failed'
      2. When the split request or split status request returns error code
    */

    if (this.state.splitFailed) {
      return (
        <Alert
          element={getSplitFailedElem(this.props.experimentId, this.props.splitInfo)}
          type='error'
          showAlert={true}
          onClose={this.closeSplitFailedAlert}
        />
      );
    }

    return (
      <Alert
        message={this.props.error}
        type='error'
        showAlert={true}
        onClose={setModelCreateError}
      />
    );
  }

  render() {
    return (
      <div className="split-data-step">
        <h3>Split Data </h3>
        <br />
        {this.renderSplitBtn()}
        {this.renderSplitInfo()}
        {this.renderError()}
      </div>
    );
  }
}

const mapStateToSplitDataStepProps = (state) => {
  let {model_create, experiments_create} = state;
  let {splitInfo = {}, error} = model_create;
  return {
    splitInfo,
    experimentId: experiments_create.name,
    error
  };
};
const ConnectedSplitDataStep = connect(mapStateToSplitDataStepProps)(SplitDataStep);

function ProvidedSplitDataStep() {
  return (
    <Provider store={createExperimentStore}>
      <ConnectedSplitDataStep />
    </Provider>
  );
}
export default ProvidedSplitDataStep;

