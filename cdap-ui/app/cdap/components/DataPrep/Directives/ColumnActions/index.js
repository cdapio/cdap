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

import React, {Component} from 'react';
import { DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import IconSVG from 'components/IconSVG';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

require('./ColumnActions.scss');

const ColumnDirectives = [
  {
    name: 'bulkset',
    label: 'Bulk Set',
    component: '<h1> Bulk Set </h1>'
  },
  {
    name: 'cleanse',
    label: 'Cleanse',
    component: '<h1> Cleanse </h1>'
  }
];

export default class ColumnActions extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeDirective: null
    };
    this.setActiveDirective = this.setActiveDirective.bind(this);
  }

  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {},
        (err) => {
          console.log('Error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }

  setActiveDirective(index) {
    let activeDirective = ColumnDirectives[index];
    if (activeDirective.name === 'cleanse') {
      this.applyDirective('cleanse-column-names');
      return;
    }
    if (!activeDirective || !activeDirective.name || !activeDirective.component) {
      return;
    }
    this.setState({
      activeDirective: activeDirective.component
    });
  }

  renderActiveDirective() {
    return (
      this.state.activeDirective
    );
  }
  render() {
    return (
      <UncontrolledDropdown
        className="collapsed-dropdown-toggle columns-actions-dropdown"
      >
        <DropdownToggle>
          <span>Column Actions</span>
          <IconSVG name="icon-chevron-down" />
        </DropdownToggle>
        <DropdownMenu>
          {
            ColumnDirectives.map((directive, i) => {
              return (
                <DropdownItem
                  title={directive.name}
                  onClick={this.setActiveDirective.bind(this, i)}
                >
                  {directive.label}
                </DropdownItem>
              );
            })
          }
        </DropdownMenu>
        {this.renderActiveDirective}
      </UncontrolledDropdown>
    );
  }
}
