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
import {Provider} from 'react-redux';
import PropTypes from 'prop-types';
import {Observable} from 'rxjs/Observable';
import {isDescendant} from 'services/helpers';
import PipelineConfigurationsStore, {TAB_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import ConfigurationsSidePanel from 'components/PipelineConfigurations/ConfigurationsSidePanel';
import ConfigurationsContent from 'components/PipelineConfigurations/ConfigurationsContent';
import IconSVG from 'components/IconSVG';
require('./PipelineConfigurations.scss');

export default class PipelineConfigurations extends Component {
  static propTypes = {
    onClose: PropTypes.func,
    isDetailView: PropTypes.bool,
    isPreview: PropTypes.bool,
    isBatch: PropTypes.bool,
    pipelineName: PropTypes.string,
    config: PropTypes.object
  };

  static defaultProps = {
    isDetailView: false,
    isPreview: false,
    isBatch: true
  };

  state = {
    activeTab: TAB_OPTIONS.RUNTIME_ARGS,
    showAdvancedTabs: false
  }

  componentWillMount() {
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.INITIALIZE_CONFIG,
      payload: {...this.props.config}
    });
  }

  componentDidMount() {
    if (!this.props.isDetailView) {
      return;
    }

    this.documentClick$ = Observable.fromEvent(document, 'click')
    .subscribe((e) => {
      if (!this.configModeless || isDescendant(this.configModeless, e.target)) {
        return;
      }

      this.props.onClose();
    });
  }

  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.unsubscribe();
    }
  }

  setActiveTab = (tab) => {
    this.setState({
      activeTab: tab
    });
  }

  toggleAdvancedTabs = () => {
    this.setState({
      showAdvancedTabs: !this.state.showAdvancedTabs
    });
  }

  renderHeader() {
    return (
      <div className="pipeline-configurations-header">
        <h3 className="modeless-title">
          Configure
          {
            this.props.pipelineName.length ?
              ` ${this.props.pipelineName}`
            :
              null
          }
        </h3>
        <div className="btn-group">
          <a
            className="btn"
            onClick={this.props.onClose}
          >
            <IconSVG name="icon-close" />
          </a>
        </div>
      </div>
    );
  }

  render() {
    return (
      <Provider store={PipelineConfigurationsStore}>
        <div
          className="pipeline-configurations-content"
          ref={(ref) => this.configModeless = ref}
        >
          {this.renderHeader()}
          <div className="pipeline-configurations-body">
            <ConfigurationsSidePanel
              isDetailView={this.props.isDetailView}
              isPreview={this.props.isPreview}
              isBatch={this.props.isBatch}
              activeTab={this.state.activeTab}
              onTabChange={this.setActiveTab}
              showAdvancedTabs={this.state.showAdvancedTabs}
              toggleAdvancedTabs={this.toggleAdvancedTabs}
            />
            <ConfigurationsContent
              activeTab={this.state.activeTab}
              isBatch={this.props.isBatch}
              isDetailView={this.props.isDetailView}
            />
          </div>
        </div>
      </Provider>
    );
  }
}

