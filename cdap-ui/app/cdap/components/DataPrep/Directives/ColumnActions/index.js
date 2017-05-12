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
    this.setActiveModal = this.setActiveModal.bind(this);
  }
  setActiveModal(name) {
    console.log('Setting ', name);
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
            ColumnDirectives.map((directive) => {
              return (
                <DropdownItem
                  title={directive.name}
                  onClick={this.setActiveModal.bind(this, directive.name)}
                >
                  {directive.label}
                </DropdownItem>
              );
            })
          }
        </DropdownMenu>
      </UncontrolledDropdown>
    );
  }
}
