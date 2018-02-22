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
import myPreferenceApi from 'api/preference';
import {getCurrentNamespace} from 'services/NamespaceStore';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import {applyRuntimeArgs, revertRuntimeArgsToSavedValues} from 'components/PipelineConfigurations/Store/ActionCreator';
import isEqual from 'lodash/isEqual';

const getPrefsRelevantToMacros = (resolvedPrefs = {}, macrosMap = {}) => {
  let relevantPrefs = {};
  for (let pref in resolvedPrefs) {
    if (resolvedPrefs.hasOwnProperty(pref) && macrosMap.hasOwnProperty(pref)) {
      relevantPrefs[pref] = resolvedPrefs[pref];
    }
  }
  return relevantPrefs;
};

export default class PipelineConfigureButton extends Component {
  static propTypes = {
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    config: PropTypes.object,
    macrosMap: PropTypes.array,
    runtimeArgs: PropTypes.array
  };

  state = {
    showModeless: false
  };

  getRuntimeArgumentsAndToggleModeless = () => {
    if (Object.keys(this.props.macrosMap).length !== 0 && !this.state.showModeless) {
      myPreferenceApi
        .getAppPreferencesResolved({
          namespace: getCurrentNamespace(),
          appId: this.props.pipelineName
        })
        .subscribe(res => {
          let relevantPrefs = getPrefsRelevantToMacros(res, this.props.macrosMap);
          let storeState = PipelineConfigurationsStore.getState();

          // If preferences have changed, then update macro values with new preferences.
          // Otherwise, keep the values as they are
          if (!storeState || (storeState && !isEqual(relevantPrefs, storeState.resolvedMacros))) {
            let resolvedMacros = {...this.props.macrosMap, ...relevantPrefs};

            PipelineConfigurationsStore.dispatch({
              type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
              payload: { resolvedMacros }
            });
            applyRuntimeArgs();
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
        className={classnames("btn pipeline-configure-btn", {"btn-select" : this.state.showModeless})}
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
      <div className="pipeline-configure">
        {this.renderConfigureButton()}
        {
          this.state.showModeless ?
            <PipelineConfigurations
              onClose={this.toggleModeless}
              isDetailView={true}
              isBatch={this.props.isBatch}
              pipelineName={this.props.pipelineName}
              config={this.props.config}
            />
          :
            null
        }
      </div>
    );
  }
}
