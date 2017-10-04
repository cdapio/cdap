/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component } from 'react';
import CollapsibleSidebar from 'components/CollapsibleSidebar';
import classnames from 'classnames';
import PipelineListTab from 'components/PipelineTriggers/PipelineListTab';
import EnabledTriggersTab from 'components/PipelineTriggers/EnabledTriggersTab';
import {fetchTriggersAndApps} from 'components/PipelineTriggers/store/PipelineTriggersActionCreator';
import PipelineTriggersActions from 'components/PipelineTriggers/store/PipelineTriggersActions';
import PipelineTriggersStore from 'components/PipelineTriggers/store/PipelineTriggersStore';
import NamespaceStore from 'services/NamespaceStore';
import {Provider} from 'react-redux';
import T from 'i18n-react';

const PREFIX = 'features.PipelineTriggers';

require('./PipelineTriggers.scss');

export default class PipelineTriggers extends Component {
  constructor(props) {
    super(props);

    let initState = PipelineTriggersStore.getState().triggers;

    let count = initState.enabledTriggersCount || 0;

    let updateOnce = false;

    this.state = {
      activeTab: count === 0 ? 1 : 0,
      tabText: `${PREFIX}.collapsedTabLabel`,
      enabledTriggersCount: count
    };

    this.onToggleSidebar = this.onToggleSidebar.bind(this);

    this.sub = PipelineTriggersStore.subscribe(() => {
      let state = PipelineTriggersStore.getState().triggers;
      if (state.enabledTriggers.length === this.state.enabledTriggersCount) { return; }

      // This is to set the default tab. During initial construction, the store
      // has not been updated with data, therefore the count is not accurate.
      if (!updateOnce) {
        this.setState({
          enabledTriggersCount: state.enabledTriggers.length,
          activeTab: state.enabledTriggers.length === 0 ? 1 : 0
        });

        updateOnce = true;
        return;
      }

      this.setState({
        enabledTriggersCount: state.enabledTriggers.length
      });
    });
  }

  componentWillMount() {
    PipelineTriggersStore.dispatch({
      type: PipelineTriggersActions.setPipelineName,
      payload: {
        pipelineName: this.props.pipelineName
      }
    });

    let namespace = NamespaceStore.getState().selectedNamespace;
    fetchTriggersAndApps(this.props.pipelineName, namespace);
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
  }

  onToggleSidebar(isExpanded) {
    this.setState({
      tabText: isExpanded ? `${PREFIX}.expandedTabLabel` : `${PREFIX}.collapsedTabLabel`
    });
  }

  setTab(tab) {
    this.setState({activeTab: tab});
  }

  renderTabContent() {
    if (this.state.activeTab === 0) {
      return <EnabledTriggersTab />;
    } else if (this.state.activeTab === 1) {
      return <PipelineListTab />;
    }
  }

  render() {
    return (
      <CollapsibleSidebar
        position="left"
        toggleTabLabel={T.translate(`${this.state.tabText}`, {count: this.state.enabledTriggersCount})}
        backdrop={false}
        onToggle={this.onToggleSidebar}
      >
        <Provider store={PipelineTriggersStore}>
          <div className="pipeline-triggers-content">
            <div className="tab-headers">
              <div
                className={classnames('tab', {'active': this.state.activeTab === 0})}
                onClick={this.setTab.bind(this, 0)}
              >
                {T.translate(`${PREFIX}.EnabledTriggers.tabLabel`, {count: this.state.enabledTriggersCount})}
              </div>

              <div
                className={classnames('tab', {'active': this.state.activeTab === 1})}
                onClick={this.setTab.bind(this, 1)}
              >
                {T.translate(`${PREFIX}.SetTriggers.tabLabel`)}
              </div>
            </div>

            {this.renderTabContent()}
          </div>
        </Provider>
      </CollapsibleSidebar>
    );
  }
}

PipelineTriggers.propTypes = {
  pipelineName: PropTypes.string.isRequired,
  namespace: PropTypes.string.isRequired
};
