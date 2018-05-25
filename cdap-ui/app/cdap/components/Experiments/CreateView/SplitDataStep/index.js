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
      Failed to split: Please check {" "}
      <a href={getSplitLogsUrl(experimentId, splitInfo)} target="_blank"> Logs </a>{" "}
      for more information
    </span>
  );
};

class SplitDataStep extends Component {
  state = {
    shouldShowSplitFailedAlert: false
  };

  static propTypes = {
    splitInfo: PropTypes.object,
    schema: PropTypes.object,
    createSplitAndUpdateStatus: PropTypes.func,
    setSplitFinalized: PropTypes.func,
    setModelCreateError: PropTypes.func,
    experimentId: PropTypes.string,
    error: PropTypes.string
  };

  componentWillReceiveProps(nextProps) {
    if (this.props.splitInfo.status && ['CREATING', 'Splitting'].indexOf(this.props.splitInfo.status) !== -1 && nextProps.splitInfo.status === 'Failed') {
      this.setState({
        shouldShowSplitFailedAlert: true
      });
    }
  }

  closeSplitFailedAlert = () => {
    this.setState({
      shouldShowSplitFailedAlert: false
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
            onClick={this.props.createSplitAndUpdateStatus}
          >
            Split data Randomly and verify sample
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

  render() {
    let splitStatus = this.props.splitInfo.status || 'Complete';

    return (
      <div className="split-data-step">
        <h3>Split Data </h3>
        <div>Create test dataset for this model.</div>
        <br />
        {this.renderSplitBtn()}
        {
          Object.keys(this.props.splitInfo).length && splitStatus === 'Complete' ? (
            <div className="action-button-group">
              <SplitInfo />
              <button
                className="btn btn-primary"
                onClick={this.props.setSplitFinalized}
                disabled={splitStatus !== 'Complete'}
              >
                Done
              </button>
              <span> Next, Select a machine learning algorithm </span>
            </div>
          ) : null
        }
        {
          this.props.error ?
            <Alert
              message={this.props.error}
              type='error'
              showAlert={true}
              onClose={this.props.setModelCreateError}
            />
          :
            null
        }
        {
          this.state.shouldShowSplitFailedAlert ?
            <Alert
              element={getSplitFailedElem(this.props.experimentId, this.props.splitInfo)}
              type='error'
              showAlert={true}
              onClose={this.closeSplitFailedAlert}
            />
          :
            null
        }
      </div>
    );
  }
}

const mapStateToSplitDataStepProps = (state) => {
  let {model_create, experiments_create} = state;
  let {splitInfo = {}, error} = model_create;
  return {
    splitInfo,
    schema: splitInfo.schema,
    experimentId: experiments_create.name,
    error
  };
};
const mapDispatchToSplitDataStepProps = () => {
  return {
    createSplitAndUpdateStatus,
    setSplitFinalized,
    setModelCreateError
  };
};
const ConnectedSplitDataStep = connect(mapStateToSplitDataStepProps, mapDispatchToSplitDataStepProps)(SplitDataStep);
function ProvidedSplitDataStep() {
  return (
    <Provider store={createExperimentStore}>
      <ConnectedSplitDataStep />
    </Provider>
  );
}
export default ProvidedSplitDataStep;

