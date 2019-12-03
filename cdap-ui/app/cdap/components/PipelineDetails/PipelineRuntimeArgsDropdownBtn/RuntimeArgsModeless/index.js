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
import React, { PureComponent } from 'react';
import RuntimeArgsTabContent from 'components/PipelineDetails/PipelineRuntimeArgsDropdownBtn/RuntimeArgsKeyValuePairWrapper';
import {
  updatePreferences,
  runPipeline,
} from 'components/PipelineConfigurations/Store/ActionCreator';
import BtnWithLoading from 'components/BtnWithLoading';
import PipelineRunTimeArgsCounter from 'components/PipelineDetails/PipelineRuntimeArgsCounter';
import { connect } from 'react-redux';
import isEmpty from 'lodash/isEmpty';
import { convertKeyValuePairsToMap } from 'services/helpers';
import Popover from 'components/Popover';
import T from 'i18n-react';
require('./RuntimeArgsModeless.scss');

const I18N_PREFIX =
  'features.PipelineDetails.PipelineRuntimeArgsDropdownBtn.RuntimeArgsTabContent.RuntimeArgsModeless';
class RuntimeArgsModeless extends PureComponent {
  static propTypes = {
    runtimeArgs: PropTypes.object,
    onClose: PropTypes.func.isRequired,
  };

  state = {
    saving: false,
    savedSuccessMessage: null,
    savingAndRun: false,
    error: null,
  };

  componentWillReceiveProps() {
    this.setState({
      savedSuccessMessage: null,
    });
  }

  toggleSaving = () => {
    this.setState({
      saving: !this.state.saving,
    });
  };

  toggleSavingAndRun = () => {
    this.setState({
      savingAndRun: !this.state.savingAndRun,
    });
  };

  saveRuntimeArgs = () => {
    this.toggleSaving();
    updatePreferences().subscribe(
      () => {
        this.setState({
          savedSuccessMessage: 'Runtime arguments saved successfully',
          saving: false,
        });
      },
      (err) => {
        this.setState({
          error: err.response || JSON.stringify(err),
          saving: false,
        });
      }
    );
  };

  saveRuntimeArgsAndRun = () => {
    this.toggleSavingAndRun();
    let { runtimeArgs } = this.props;
    // Arguments with empty values are assumed to be provided from the pipeline
    runtimeArgs.pairs = runtimeArgs.pairs.filter((runtimeArg) => !runtimeArg.value);
    let runtimeArgsMap = convertKeyValuePairsToMap(runtimeArgs.pairs);
    runPipeline(runtimeArgsMap);
    this.props.onClose();
  };

  render() {
    const SaveBtn = () => {
      return (
        <BtnWithLoading
          loading={this.state.saving}
          className="btn btn-primary"
          onClick={this.saveRuntimeArgs}
          disabled={this.state.saving || !isEmpty(this.state.savedSuccessMessage)}
          label="Save"
        />
      );
    };
    const RunBtn = () => {
      return (
        <BtnWithLoading
          loading={this.state.savingAndRun}
          className="btn btn-secondary"
          onClick={this.saveRuntimeArgsAndRun}
          disabled={this.state.saving}
          label="Run"
        />
      );
    };
    return (
      <div className="runtime-args-modeless">
        <div className="arguments-label">{T.translate(`${I18N_PREFIX}.specifyArgs`)}</div>
        <RuntimeArgsTabContent />
        <div className="tab-footer">
          <div className="btns-container">
            <Popover target={SaveBtn} placement="left" showOn="Hover">
              {T.translate(`${I18N_PREFIX}.saveBtnPopover`)}
            </Popover>
            <Popover target={RunBtn} showOn="Hover" placement="right">
              {T.translate(`${I18N_PREFIX}.runBtnPopover`)}
            </Popover>
            {!isEmpty(this.state.savedSuccessMessage) ? (
              <span className="text-success">{this.state.savedSuccessMessage}</span>
            ) : null}
          </div>
          <PipelineRunTimeArgsCounter />
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    runtimeArgs: state.runtimeArgs,
  };
};

const ConnectedRuntimeArgsModeless = connect(mapStateToProps)(RuntimeArgsModeless);

export default ConnectedRuntimeArgsModeless;
