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

import classnames from 'classnames';
import * as React from 'react';
import IconSVG from 'components/IconSVG';

require('./DataPrepBrowserTopPanel.scss');

interface IDataprepBrowserTopPanel {
  allowSidePanelToggle: boolean;
  toggle: (e: React.MouseEvent<HTMLElement>) => void;
  browserTitle: React.ReactNode;
}
export default class DataprepBrowserTopPanel extends React.PureComponent<IDataprepBrowserTopPanel> {
  public render() {
    return (
      <div className="dataprep-browser-top-panel">
        <div className="title">
          <h5>
            <span
              className={classnames('fa fa-fw', {
                disabled: !this.props.allowSidePanelToggle,
              })}
              onClick={this.props.toggle}
            >
              <IconSVG name="icon-bars" />
            </span>

            <span>{this.props.browserTitle}</span>
          </h5>
        </div>
      </div>
    );
  }
}
