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
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import PipelineConfigurations from 'components/PipelineConfigurations';
import {MyPreferenceApi} from 'api/preference';
import {getCurrentNamespace} from 'services/NamespaceStore';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {revertRuntimeArgsToSavedValues, getMacrosResolvedByPrefs} from 'components/PipelineConfigurations/Store/ActionCreator';
import isEqual from 'lodash/isEqual';

export default class PipelineConfigureButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    resolvedMacros: PropTypes.object,
    runtimeArgs: PropTypes.array
  };

  state = {
    showModeless: false
  };

  getRuntimeArgumentsAndToggleModeless = () => {
    if (Object.keys(this.props.resolvedMacros).length !== 0 && !this.state.showModeless) {
      MyPreferenceApi
        .getAppPreferencesResolved({
          namespace: getCurrentNamespace(),
          appId: this.props.pipelineName
        })
        .subscribe(res => {
          let newResolvedMacros = getMacrosResolvedByPrefs(res, this.props.resolvedMacros);

          // If preferences have changed, then update macro values with new preferences.
          // Otherwise, keep the values as they are
          if (!isEqual(newResolvedMacros, this.props.resolvedMacros)) {
            PipelineConfigurationsStore.dispatch({
              type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
              payload: { resolvedMacros: newResolvedMacros }
            });
          }

          this.toggleModeless();
        }, (err) => {
          console.log(err);
        });
    } else {
      this.toggleModeless();
    }
  };

  toggleModeless = () => {
    if (this.state.showModeless) {
      revertRuntimeArgsToSavedValues();
    }
    this.setState({
      showModeless: !this.state.showModeless
    });
  };

  renderConfigureButton() {
    return (
      <div
        onClick={this.getRuntimeArgumentsAndToggleModeless}
        className={classnames("btn pipeline-action-btn pipeline-configure-btn", {"btn-select" : this.state.showModeless})}
      >
        <div className="btn-container">
          <IconSVG
            name="icon-sliders"
            className="configure-icon"
          />
          <div className="button-label">Configure</div>
        </div>
      </div>
    );
  }

  render() {
    return (
      <div className="pipeline-action-container pipeline-configure-container">
        {this.renderConfigureButton()}
        {
          this.state.showModeless ?
            <PipelineConfigurations
              onClose={this.toggleModeless}
              isDetailView={true}
              isBatch={this.props.isBatch}
              pipelineName={this.props.pipelineName}
            />
          :
            null
        }
      </div>
    );
  }
}
