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
import Bulkset from 'components/DataPrep/Directives/ColumnActions/Bulkset';
import ReplaceColumns from 'components/DataPrep/Directives/ColumnActions/ReplaceColumns';
import T from 'i18n-react';
require('./ColumnActions.scss');

const PREFIX = 'features.DataPrep.Directives.ColumnActions';
export default class ColumnActions extends Component {
  constructor(props) {
    super(props);
    this.resetActiveDirective = this.resetActiveDirective.bind(this);
    this.state = {
      activeDirective: null,
      columnDirectives: [
        {
          name: 'bulkset',
          label: T.translate(`${PREFIX}.actions.bulkset`),
          component: <Bulkset onClose={this.resetActiveDirective} />
        },
        {
          name: 'cleanse',
          label: T.translate(`${PREFIX}.actions.cleanse`)
        },
        {
          name: 'replacecolumns',
          label: T.translate(`${PREFIX}.actions.replaceColumns`),
          component: <ReplaceColumns onClose={this.resetActiveDirective} />
        }
      ]
    };
    this.setActiveDirective = this.setActiveDirective.bind(this);
    this.renderActiveDirective = this.renderActiveDirective.bind(this);
  }

  resetActiveDirective() {
    this.setState({
      activeDirective: null
    });
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
    let currentDirective = this.state.columnDirectives[index];
    if (currentDirective.name === 'cleanse') {
      this.applyDirective('cleanse-column-names');
      return;
    }
    if (!currentDirective || !currentDirective.name || !currentDirective.component) {
      return;
    }
    this.setState({
      activeDirective: currentDirective.component
    });
  }

  renderActiveDirective() {
    return (
      this.state.activeDirective
    );
  }
  render() {
    return (
      <div className="columns-actions-dropdown">
        <UncontrolledDropdown
          className="collapsed-dropdown-toggle"
        >
          <DropdownToggle>
            <span>{T.translate('features.DataPrep.Directives.ColumnActions.label')}</span>
            <IconSVG name="icon-chevron-down" />
          </DropdownToggle>
          <DropdownMenu right>
            {
              this.state.columnDirectives.map((directive, i) => {
                return (
                  <DropdownItem
                    key={i}
                    title={directive.name}
                    onClick={this.setActiveDirective.bind(this, i)}
                  >
                    {directive.label}
                  </DropdownItem>
                );
              })
            }
          </DropdownMenu>
        </UncontrolledDropdown>
        {this.renderActiveDirective()}
      </div>
    );
  }
}
