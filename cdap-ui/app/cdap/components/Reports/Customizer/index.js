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
import IconSVG from 'components/IconSVG';
import ColumnsSelector from 'components/Reports/Customizer/ColumnsSelector';
import TimeRangeSelector from 'components/Reports/Customizer/TimeRangeSelector';
import AppTypeSelector from 'components/Reports/Customizer/AppTypeSelector';
import ActionButtons from 'components/Reports/Customizer/ActionButtons';
import StatusSelector from 'components/Reports/Customizer/StatusSelector';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.Reports.Customizer';

require('./Customizer.scss');

export default class Customizer extends Component {
  state = {
    isCollapsed: false
  };

  toggleCollapsed = () => {
    this.setState({
      isCollapsed: !this.state.isCollapsed
    });
  };

  renderCollapsedDetail = () => {
    if (this.state.isCollapsed) { return null; }

    return (
      <div>
        <div className="options-container">
          <AppTypeSelector />
          <StatusSelector />
          <ColumnsSelector />
          <TimeRangeSelector />
        </div>

        <ActionButtons />
      </div>
    );
  };

  render() {
    return (
      <div className={classnames('customizer-container', {collapsed: this.state.isCollapsed})}>
        <div className="collapsed-toggle-container">
          <div
            className="toggle"
            onClick={this.toggleCollapsed}
          >
            <IconSVG name={this.state.isCollapsed ? 'icon-caret-right' : 'icon-caret-down'} />
            {
              this.state.isCollapsed ?
                T.translate(`${PREFIX}.show`)
              :
                T.translate(`${PREFIX}.hide`)
            }
          </div>
        </div>

        {this.renderCollapsedDetail()}
      </div>
    );
  }
}
