/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes, Component} from 'react';
var classnames = require('classnames');
require('./TabHead.scss');
export default class TabHead extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeTab: this.props.activeTab,
      layout: this.props.layout
    };
  }
  componentWillReceiveProps(newProps) {
    this.setState({
      activeTab: newProps.activeTab
    });
  }
  render() {
    return (
      <div
        className={classnames("cask-tab-head", {
          'active-tab': this.state.activeTab,
          'vertical': this.state.layout === 'vertical',
          'horizontal': this.state.layout === 'horizontal'
        })}
        onClick={this.props.onClick}
      >
        {this.props.children}
      </div>
    );
  }
}
TabHead.propTypes = {
  children: PropTypes.node,
  onClick: PropTypes.func,
  activeTab: PropTypes.bool,
  layout: PropTypes.string
};
