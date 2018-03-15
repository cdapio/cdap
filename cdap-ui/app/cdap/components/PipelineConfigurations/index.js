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
import PipelineConfigurationsStore, {TAB_OPTIONS} from 'components/PipelineConfigurations/Store';
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
    action: PropTypes.string,
    isHistoricalRun: PropTypes.bool
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

  componentDidMount() {
    if (!this.props.isDetailView) {
      return;
    }

    this.documentClick$ = Observable.fromEvent(document, 'click')
    .subscribe((e) => {
      if (!this.configModeless || isDescendant(this.configModeless, e.target) || document.getElementsByClassName('post-run-actions-modal').length > 0) {
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
    let headerLabel;
    if (this.props.isHistoricalRun) {
      headerLabel = 'Run Configurations';
    } else {
      headerLabel = 'Configure';
      if (this.props.pipelineName.length) {
        headerLabel += ` "${this.props.pipelineName}"`;
      }
    }
    return (
      <div className="pipeline-configurations-header modeless-header">
        <div className="modeless-title">
          {headerLabel}
        </div>
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
          className="pipeline-configurations-content modeless-container"
          ref={(ref) => this.configModeless = ref}
        >
          {this.renderHeader()}
          <div className="pipeline-configurations-body modeless-content">
            <ConfigurationsSidePanel
              isDetailView={this.props.isDetailView}
              isPreview={this.props.isPreview}
              isBatch={this.props.isBatch}
              isHistoricalRun={this.props.isHistoricalRun}
              activeTab={this.state.activeTab}
              onTabChange={this.setActiveTab}
              showAdvancedTabs={this.state.showAdvancedTabs}
              toggleAdvancedTabs={this.toggleAdvancedTabs}
            />
            <ConfigurationsContent
              activeTab={this.state.activeTab}
              isBatch={this.props.isBatch}
              isDetailView={this.props.isDetailView}
              isHistoricalRun={this.props.isHistoricalRun}
              onClose={this.props.onClose}
              action={this.props.action}
            />
          </div>
        </div>
      </Provider>
    );
  }
}
