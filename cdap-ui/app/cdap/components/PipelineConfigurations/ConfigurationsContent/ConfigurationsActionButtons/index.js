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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {TAB_OPTIONS} from 'components/PipelineConfigurations/Store';
import {applyRuntimeArgs, updatePipeline, runPipeline} from 'components/PipelineConfigurations/Store/ActionCreator';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
require('./ConfigurationsActionButtons.scss');

const PREFIX = 'features.PipelineConfigurations.ActionButtons';

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: state.runtimeArgs,
    validToSave: state.validToSave,
    pipelineEdited: state.pipelineEdited,
    updatingPipeline: ownProps.updatingPipeline,
    saveAndClose: ownProps.saveAndClose,
    activeTab: ownProps.activeTab,
  };
};

const ConfigActionButtons = ({runtimeArgs, validToSave, pipelineEdited, updatingPipeline, updatingPipelineAndRunning, saveAndClose, saveAndRun, activeTab}) => {
  return (
    <div className="configuration-step-navigation">
      <div className="apply-run-container">
        <button
          className="btn btn-primary apply-run"
          disabled={updatingPipelineAndRunning || !validToSave}
          onClick={saveAndRun.bind(this, pipelineEdited)}
        >
          <span>Save and Run</span>
          {
            updatingPipelineAndRunning ?
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
            :
              null
          }
        </button>
        <button
          className="btn btn-secondary"
          disabled={updatingPipeline || !validToSave}
          onClick={saveAndClose.bind(this, pipelineEdited)}
        >
          {
            updatingPipeline ?
              (
                <span>
                  Saving
                  <IconSVG
                    name="icon-spinner"
                    className="fa-spin"
                  />
                 </span>
              )
            :
              <span>Save</span>
          }
        </button>
        {
          activeTab === TAB_OPTIONS.RUNTIME_ARGS ?
            (
              <span className="num-runtime-args">
                {T.translate(`${PREFIX}.runtimeArgsCount`, {context: runtimeArgs.pairs.length})}
              </span>
            )
          :
            null
        }
      </div>
    </div>
  );
};

ConfigActionButtons.propTypes = {
  activeTab: PropTypes.string,
  runtimeArgs: PropTypes.object,
  validToSave: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  updatingPipeline: PropTypes.bool,
  updatingPipelineAndRunning: PropTypes.bool,
  saveAndClose: PropTypes.func,
  saveAndRun: PropTypes.func
};

const ConnectedConfigActionButtons = connect(mapStateToProps)(ConfigActionButtons);

export default class ConfigurationsActionButtons extends Component {
  state = {
    updatingPipeline: false,
    updatingPipelineAndRunning: false
  };

  static propTypes = {
    onClose: PropTypes.func,
    activeTab: PropTypes.string
  }

  saveAndRun = (pipelineEdited) => {
    applyRuntimeArgs();
    if (pipelineEdited) {
      this.setState({
        updatingPipelineAndRunning: true
      });
      updatePipeline()
      .subscribe(() => {
        this.setState({
          updatingPipelineAndRunning: false
        });
        this.props.onClose();
        runPipeline();
      }, (err) => {
        console.log(err);
        this.setState({
          updatingPipelineAndRunning: false
        });
      });
    } else {
      runPipeline();
      this.props.onClose();
    }
  }

  saveAndClose = (pipelineEdited) => {
    applyRuntimeArgs();
    if (pipelineEdited) {
      this.setState({
        updatingPipeline: true
      });
      updatePipeline()
      .subscribe(() => {
        this.props.onClose();
      }, (err) => {
        console.log(err);
      }, () => {
        this.setState({
          updatingPipeline: false
        });
      });
    } else {
      this.props.onClose();
    }
  };

  render() {
    return (
      <ConnectedConfigActionButtons
        updatingPipeline={this.state.updatingPipeline}
        updatingPipelineAndRunning={this.state.updatingPipelineAndRunning}
        saveAndClose={this.saveAndClose}
        saveAndRun={this.saveAndRun}
        activeTab={this.props.activeTab}
      />
    );
  }
}
