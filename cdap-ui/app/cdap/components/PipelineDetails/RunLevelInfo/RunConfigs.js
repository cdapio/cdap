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
import IconSVG from 'components/IconSVG';
import { convertMapToKeyValuePairsObj } from 'components/KeyValuePairs/KeyValueStoreActions';
import PipelineConfigurationsStore, {
  ACTIONS as PipelineConfigurationsActions,
} from 'components/PipelineConfigurations/Store';
import { reset } from 'components/PipelineConfigurations/Store/ActionCreator';
import {
  objectQuery,
  reverseArrayWithoutMutating,
  isNilOrEmpty,
  preventPropagation,
} from 'services/helpers';
import classnames from 'classnames';
import Popover from 'components/Popover';
import PipelineModeless from 'components/PipelineDetails/PipelineModeless';
import T from 'i18n-react';
import { Provider } from 'react-redux';
import findIndex from 'lodash/findIndex';
import CopyableID from 'components/CopyableID';
import PipelineDetailStore from 'components/PipelineDetails/store';
import PipelineRunTimeArgsCounter from 'components/PipelineDetails/PipelineRuntimeArgsCounter';
import { getFilteredRuntimeArgs } from 'components/PipelineConfigurations/Store/ActionCreator';

const PREFIX = 'features.PipelineDetails.RunLevel';

export default class RunConfigs extends Component {
  static propTypes = {
    currentRun: PropTypes.object,
    runs: PropTypes.array,
    pipelineType: PropTypes.string,
    pipelineName: PropTypes.string,
  };

  state = {
    showModeless: false,
    runtimeArgs: {
      pairs: [],
    },
  };

  runtimeArgsMap = {};

  componentWillReceiveProps() {
    this.getRuntimeArgsAndToggleModeless();
  }

  componentDidMount() {
    this.getRuntimeArgsAndToggleModeless();
  }

  getRuntimeArgsAndToggleModeless = () => {
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_MODELESS_OPEN_STATUS,
      payload: { open: false },
    });

    let runtimeArgs = objectQuery(this.props.currentRun, 'properties', 'runtimeArgs') || '';
    try {
      runtimeArgs = JSON.parse(runtimeArgs);
      delete runtimeArgs[''];
    } catch (e) {
      console.log('ERROR: Cannot parse runtime arguments');
      runtimeArgs = {};
    }

    this.runtimeArgsMap = JSON.stringify(runtimeArgs, null, 2);
    runtimeArgs = getFilteredRuntimeArgs(convertMapToKeyValuePairsObj(runtimeArgs));

    this.setState({
      runtimeArgs,
    });

    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_PIPELINE_VISUAL_CONFIGURATION,
      payload: {
        pipelineVisualConfiguration: {
          isHistoricalRun: true,
        },
      },
    });
  };

  toggleModeless = (showModeless) => {
    if (showModeless === this.state.showModeless) {
      return;
    }
    this.setState(
      {
        showModeless: showModeless || !this.state.showModeless,
      },
      () => {
        if (!this.state.showModeless) {
          reset();
          /*
          Reset overrides existing pipeline config as well instead of overwriting only the
          historical run information. We should either have seperate stores for run information
          and pipeline information or don't reset the pipeline config if historical
          run information has to be reset.
        */
          PipelineConfigurationsStore.dispatch({
            type: PipelineConfigurationsActions.INITIALIZE_CONFIG,
            payload: { ...PipelineDetailStore.getState().config },
          });
        } else {
          this.getRuntimeArgsAndToggleModeless();
        }
      }
    );
  };

  isRuntimeArgsEmpty = () => {
    if (!this.state.runtimeArgs.pairs.length) {
      return true;
    }
    if (this.state.runtimeArgs.pairs.length === 1) {
      if (
        isNilOrEmpty(this.state.runtimeArgs.pairs[0].key) &&
        isNilOrEmpty(this.state.runtimeArgs.pairs[0].value)
      ) {
        return true;
      }
      return false;
    }
    return false;
  };

  renderRuntimeArgs = () => {
    return (
      <div className="historical-runtimeargs-keyvalues">
        <div>
          <div>Name</div>
          <div>Value</div>
        </div>
        {this.state.runtimeArgs.pairs.map((arg) => {
          return (
            <div>
              <input className="form-control" value={arg.key} disabled />
              <input className="form-control" value={arg.value} disabled />
            </div>
          );
        })}
      </div>
    );
  };

  renderRunConfigsButton = () => {
    const target = (
      <div className="run-configs-btn">
        <IconSVG name="icon-macro" />
        <div className="button-label">{T.translate(`${PREFIX}.configs`)}</div>
      </div>
    );

    let { runs, currentRun } = this.props;
    let reversedRuns = reverseArrayWithoutMutating(runs);
    let currentRunIndex = findIndex(reversedRuns, { runid: objectQuery(currentRun, 'runid') });
    const title = (
      <div className="runconfig-modeless-title">
        <div>
          {T.translate(`${PREFIX}.configsModelessTitle`, {
            currentRunIndex: currentRunIndex + 1,
          })}
        </div>
        <CopyableID
          label={T.translate(`${PREFIX}.copyRuntimeArgsBtnLabel`)}
          id={this.runtimeArgsMap}
          tooltipText={false}
        />
      </div>
    );
    return (
      <Popover
        target={() => target}
        placement="bottom"
        enableInteractionInPopover={true}
        showPopover={this.state.showModeless}
        onTogglePopover={this.toggleModeless}
        injectOnToggle={true}
        modifiers={{
          flip: {
            enabled: true,
            behavior: ['bottom', 'right'],
          },
          shift: {
            enabled: true,
          },
        }}
      >
        <Provider store={PipelineConfigurationsStore}>
          <PipelineModeless title={title} onClose={this.toggleModeless.bind(this, false)}>
            <div className="historical-runtime-args-wrapper">
              {this.renderRuntimeArgs()}
              <div className="runconfig-tab-footer">
                {this.isRuntimeArgsEmpty() ? null : (
                  <PipelineRunTimeArgsCounter runtimeArgs={this.state.runtimeArgs} />
                )}
              </div>
            </div>
          </PipelineModeless>
        </Provider>
      </Popover>
    );
  };

  render() {
    const ConfigsBtnComp = () => (
      <div className="run-configs-btn" onClick={preventPropagation}>
        <IconSVG name="icon-macro" />
        <div className="button-label">{T.translate(`${PREFIX}.configs`)}</div>
      </div>
    );

    if (!this.props.runs.length || this.isRuntimeArgsEmpty()) {
      return (
        <Popover
          target={ConfigsBtnComp}
          showOn="Hover"
          placement="bottom-end"
          className="run-info-container run-configs-container disabled"
        >
          {!this.props.runs.length
            ? T.translate(`${PREFIX}.pipelineNeverRun`)
            : T.translate(`${PREFIX}.noRuntimeArgsForRun`)}
        </Popover>
      );
    }

    return (
      <div
        className={classnames('run-info-container run-configs-container', {
          active: this.state.showModeless,
        })}
      >
        {this.renderRunConfigsButton()}
      </div>
    );
  }
}
