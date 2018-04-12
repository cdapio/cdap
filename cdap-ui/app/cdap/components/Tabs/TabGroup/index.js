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
import TabHead from 'components/Tabs/TabHead';
import IconSVG from 'components/IconSVG';

require('./TabGroup.scss');

export default class TabGroup extends Component {
  static propTypes = {
    tabGroup: PropTypes.object,
    activeTab: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    onTabClick: PropTypes.func,
    layout: PropTypes.string
  };
  state = {
    opened: this.props.tabGroup.opened || false
  };

  toggleGroup = () => {
    this.setState({
      opened: !this.state.opened
    });
  };

  renderSubTabs = () => {
    return this.props.tabGroup.subtabs.map(tab => {
      return (
        <TabHead
          layout={this.props.layout}
          key={tab.id}
          onClick={() => this.props.onTabClick(tab.id)}
          activeTab={this.props.activeTab === tab.id}
        >
          <span className="fa-fw tab-header-icon">
            <IconSVG name={tab.icon} />
          </span>
          <span title={tab.name}>{tab.name}</span>
        </TabHead>
      );
    });
  };

  render() {
    return (
      <div className="cask-tab-group cask-tab-head">
        <TabHead
          layout={this.props.layout}
          className="group-title"
          onClick={this.toggleGroup}
        >
          <span className="fa-fw tab-header-icon">
            {
              this.state.opened ?
                <IconSVG name="icon-caret-down" />
              :
                <IconSVG name="icon-caret-right" />
            }
          </span>
          <span title={this.props.tabGroup.name}>
            {this.props.tabGroup.name}
          </span>
        </TabHead>
        <div className="group-tabs">
          {
            this.state.opened ?
              this.renderSubTabs()
            :
              null
          }
        </div>
      </div>
    );
  }
}
