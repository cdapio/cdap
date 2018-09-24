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
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import ConfigModelessActionButtons from 'components/PipelineConfigurations/ConfigurationsContent/ConfigModelessActionButtons';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import ConfigurableTab from 'components/ConfigurableTab';
import TabConfig from 'components/PipelineConfigurations/TabConfig';

require('./PipelineConfigurations.scss');
require('./ConfigurationsContent/ConfigurationsContent.scss');

const PREFIX = 'features.PipelineConfigurations';

export default class PipelineConfigurations extends Component {
  static propTypes = {
    onClose: PropTypes.func,
    isDetailView: PropTypes.bool,
    isPreview: PropTypes.bool,
    isBatch: PropTypes.bool,
    isHistoricalRun: PropTypes.bool,
    action: PropTypes.string,
    pipelineName: PropTypes.string
  };

  static defaultProps = {
    isDetailView: false,
    isPreview: false,
    isBatch: true
  };

  componentDidMount() {
    if (!this.props.isDetailView) {
      return;
    }
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_MODELESS_OPEN_STATUS,
      payload: { open: true }
    });

    let {isBatch, isDetailView, isHistoricalRun, isPreview} = this.props;

    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_PIPELINE_VISUAL_CONFIGURATION,
      payload: {
        pipelineVisualConfiguration: {
          isBatch,
          isDetailView,
          isHistoricalRun,
          isPreview
        }
      }
    });

    this.storeSubscription = PipelineConfigurationsStore.subscribe(() => {
      let state = PipelineConfigurationsStore.getState();
      if (!state.modelessOpen) {
        this.props.onClose();
        this.storeSubscription();
      }
    });

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
    if (this.storeSubscription) {
      this.storeSubscription();
    }
  }

  renderHeader() {
    let headerLabel;
    if (this.props.isHistoricalRun) {
      headerLabel = T.translate(`${PREFIX}.titleHistorical`);
    } else {
      headerLabel = T.translate(`${PREFIX}.title`);
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
    let tabConfig;
    if (this.props.isBatch) {
      tabConfig = TabConfig;
    } else {
      tabConfig = {...TabConfig};
      // Don't show Alerts tab for realtime pipelines
      const alertsTabName = T.translate(`${PREFIX}.Alerts.title`);
      tabConfig.tabs = TabConfig.tabs.filter(tab => {
        return tab.name !== alertsTabName;
      });
    }
    return (
      <Provider store={PipelineConfigurationsStore}>
        <div
          className="pipeline-configurations-content modeless-container"
          ref={(ref) => this.configModeless = ref}
        >
          {this.renderHeader()}
          <div className="pipeline-config-tabs-wrapper">
            <ConfigurableTab tabConfig={tabConfig} />
            <ConfigModelessActionButtons onClose={this.props.onClose} />
          </div>
        </div>
      </Provider>
    );
  }
}
