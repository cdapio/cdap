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
    updatingPipelineAndAction: ownProps.updatingPipelineAndAction,
    saveAndClose: ownProps.saveAndClose,
    saveAndAction: ownProps.saveAndAction,
    activeTab: ownProps.activeTab,
    actionLabel: ownProps.actionLabel
  };
};

const ConfigActionButtons = ({runtimeArgs, validToSave, pipelineEdited, updatingPipeline, updatingPipelineAndAction, saveAndClose, saveAndAction, activeTab, actionLabel}) => {
  return (
    <div className="configuration-step-navigation">
      <div className="apply-run-container">
        <button
          className="btn btn-primary apply-run"
          disabled={updatingPipelineAndAction || !validToSave}
          onClick={saveAndAction.bind(this, pipelineEdited)}
        >
          <span>{actionLabel}</span>
          {
            updatingPipelineAndAction ?
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
  updatingPipelineAndAction: PropTypes.bool,
  saveAndClose: PropTypes.func,
  saveAndAction: PropTypes.func,
  actionLabel: PropTypes.string
};

const ConnectedConfigActionButtons = connect(mapStateToProps)(ConfigActionButtons);

export default class ConfigurationsActionButtons extends Component {
  state = {
    // need 2 states here instead of just 1, to determine which button to show
    // spinning wheel on
    updatingPipeline: false,
    updatingPipelineAndAction: false
  };

  static propTypes = {
    onClose: PropTypes.func,
    activeTab: PropTypes.string,
    action: PropTypes.func
  };

  static defaultProps = {
    action: runPipeline
  };

  close = () => {
    if (typeof this.props.onClose === 'function') {
      this.props.onClose();
    }
  };

  closeAndAction = () => {
    this.close();
    if (typeof this.props.action === 'function') {
      this.props.action();
    }
  };

  saveAndAction = (pipelineEdited) => {
    applyRuntimeArgs();
    if (!pipelineEdited) {
      this.closeAndAction();
      return;
    }

    this.setState({
      updatingPipelineAndAction: true
    });
    updatePipeline()
    .subscribe(() => {
      this.closeAndAction();
    }, (err) => {
      console.log(err);
    }, () => {
      this.setState({
        updatingPipelineAndAction: false
      });
    });
  }

  saveAndClose = (pipelineEdited) => {
    applyRuntimeArgs();
    if (!pipelineEdited) {
      this.close();
      return;
    }

    this.setState({
      updatingPipeline: true
    });
    updatePipeline()
    .subscribe(() => {
      this.close();
    }, (err) => {
      console.log(err);
    }, () => {
      this.setState({
        updatingPipeline: false
      });
    });
  };

  render() {
    let actionLabel = this.props.action === runPipeline ? 'Save and Run' : 'Save and Schedule';
    return (
      <ConnectedConfigActionButtons
        updatingPipeline={this.state.updatingPipeline}
        updatingPipelineAndAction={this.state.updatingPipelineAndAction}
        saveAndClose={this.saveAndClose}
        saveAndAction={this.saveAndAction}
        activeTab={this.props.activeTab}
        actionLabel={actionLabel}
      />
    );
  }
}
