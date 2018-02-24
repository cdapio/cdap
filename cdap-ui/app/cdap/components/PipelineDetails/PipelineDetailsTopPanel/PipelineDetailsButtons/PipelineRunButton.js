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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {MyProgramApi} from 'api/program';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import PipelineConfigurationsStore from 'components/PipelineConfigurations/Store';
import {GLOBALS} from 'services/global-constants';

export default class PipelineRunButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
  }

  state = {
    loading: false,
    runError: null
  };

  runPipeline = () => {
    this.setState({
      loading: true
    });
    let pipelineType = this.props.isBatch ? GLOBALS.etlDataPipeline : GLOBALS.etlDataStreams;
    let params = {
      namespace: getCurrentNamespace(),
      appId: this.props.pipelineName,
      programType: GLOBALS.programType[pipelineType],
      programId: GLOBALS.programId[pipelineType],
      action: 'start'
    };
    let runtimeArgs = convertKeyValuePairsObjToMap(PipelineConfigurationsStore.getState().runtimeArgs);
    MyProgramApi.action(params, runtimeArgs)
    .subscribe(() => {
      this.setState({
        loading: false
      });
    }, (err) => {
      this.setState({
        loading: false,
        runError: err.response || err
      });
    });
  }

  renderRunError() {
    if (!this.state.runError) {
      return null;
    }

    return (
      <Alert
        message={this.state.runError}
        type='error'
        showAlert={true}
        onClose={() => this.setState({
          runError: null
        })}
      />
    );
  }

  renderPipelineRunButton() {
    return (
      <div
        onClick={this.runPipeline}
        className="btn pipeline-run-btn"
        disabled={this.state.loading}
      >
        <div className="btn-container">
          {
            this.state.loading ?
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            :
              (
                <span>
                  <IconSVG
                    name="icon-play"
                    className="text-success"
                  />
                  <div className="button-label">
                    Run
                  </div>
                </span>
              )
          }
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-run-container">
        {this.renderRunError()}
        {this.renderPipelineRunButton()}

      </div>
    );
  }
}
