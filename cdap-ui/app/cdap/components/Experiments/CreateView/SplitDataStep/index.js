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
import createExperimentStore, {SPLIT_STATUS} from 'components/Experiments/store/createExperimentStore';
import {createSplitAndUpdateStatus, setSplitFinalized, setModelCreateError} from 'components/Experiments/store/CreateExperimentActionCreator';
import {getCurrentNamespace} from 'services/NamespaceStore';
import SplitInfo from 'components/Experiments/CreateView/SplitDataStep/SplitInfo';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import isEmpty from 'lodash/isEmpty';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.Experiments.CreateView';

require('./SplitDataStep.scss');
const getSplitLogsUrl = (experimentId, splitInfo) => {
  let splitId = splitInfo.id;
  let startTime = splitInfo.start;
  let endTime = splitInfo.end;
  let baseUrl = `/logviewer/view?namespace=${getCurrentNamespace()}&appId=ModelManagementApp&programType=spark&programId=ModelManagerService`;
  let queryParams = `&filter=${encodeURIComponent(`MDC:experiment="${experimentId}" AND MDC:split=${splitId}`)}&startTime=${startTime}&endTime=${endTime}`;
  return `${baseUrl}${queryParams}`;
};

const getSplitFailedElem = (experimentId, splitInfo) => {
  return (
    <span className="split-error-container">
      {T.translate(`${PREFIX}.failedToSplit`, {experimentId})}
      <a href={getSplitLogsUrl(experimentId, splitInfo)} target="_blank">
        {T.translate(`${PREFIX}.logs`)}
      </a>
      {T.translate(`${PREFIX}.moreInfo`)}
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
    const isCurrentSplitStatusFailed = nextProps.splitInfo.status === SPLIT_STATUS.FAILED;
    const isStartingSplitFailed = this.props.splitInfo.status === SPLIT_STATUS.CREATING && nextProps.error;
    if (isCurrentSplitStatusFailed || isStartingSplitFailed) {
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
    let isSplitComplete = [SPLIT_STATUS.COMPLETE, SPLIT_STATUS.FAILED].indexOf(splitStatus) !== -1;

    if (!isSplitCreated || (isSplitCreated && isSplitComplete)) {
      return (
        <div>
          <button
            className={classnames("btn btn-primary", {
              'btn-secondary' : isSplitCreated && isSplitComplete
            })}
            onClick={createSplitAndUpdateStatus}
          >
            {
              this.state.splitFailed ||  isSplitCreated ?
                T.translate(`${PREFIX}.resplitAndVerify`)
              :
                T.translate(`${PREFIX}.splitAndVerify`)
            }
          </button>
          {
            isSplitCreated && isSplitComplete ?
              <div className="done-action-container">
                <button
                  className="btn btn-primary"
                  onClick={setSplitFinalized}
                  disabled={splitStatus !== SPLIT_STATUS.COMPLETE}
                >
                  {T.translate('commons.doneLabel')}
                </button>
                <span>{T.translate(`${PREFIX}.nextSelect`)}</span>
              </div>
            :
              null
          }
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
          <span>{T.translate(`${PREFIX}.splitting`)}</span>
        </div>
      </button>
    );
  }

  renderSplitInfo() {
    const isSplitFailed = this.props.splitInfo.status === SPLIT_STATUS.FAILED;
    if (isEmpty(this.props.splitInfo) || !this.props.splitInfo.id || isSplitFailed) {
      return null;
    }

    return (
      <div className="action-button-group">
        <SplitInfo />
      </div>
    );
  }

  renderError() {
    if (!this.state.splitFailed) {
      return null;
    }

    /*
      We have 2 cases where we want to show an Alert on this page:
      1. When the split status request returns 200, but status is 'Failed'
      2. When the split request or split status request returns error code
    */

    if (this.props.splitInfo.id) {
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
        onClose={() => {
          this.closeSplitFailedAlert();
          setModelCreateError();
        }}
      />
    );
  }

  render() {
    return (
      <div className="split-data-step">
        <h3>{T.translate(`${PREFIX}.splitData`)}</h3>
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

