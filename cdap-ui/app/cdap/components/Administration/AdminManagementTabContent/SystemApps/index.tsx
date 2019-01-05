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

import * as React from 'react';
import Heading, { HeadingTypes } from 'components/Heading';

export default class SystemApps extends React.PureComponent {
  private tableHeaders = [
    {
      label: 'Status',
      name: 'status',
    },
    {
      label: 'Name',
      name: 'name',
    },
    {
      label: 'Provisioned',
      name: 'provisioned',
    },
    {
      label: 'Requested',
      name: 'requested',
    },
    {
      label: '',
      name: 'logs',
    },
    {
      label: '',
      name: 'action',
    },
  ];
  public render() {
    return (
      <div>
        <Heading type={HeadingTypes.h4} label={<strong>System Apps</strong>} />
        <div className="systems-apps-table">
          <div className="grid grid-container">
            <div className="grid-header">
              <div className="grid-row">
                {this.tableHeaders.map((header, i) => {
                  return <strong key={i}>{header.label}</strong>;
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
