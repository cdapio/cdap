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

import React, { Component, PropTypes } from 'react';
import {isDescendant} from 'services/helpers';
import {Popover, PopoverContent} from 'reactstrap';
import Rx from 'rx';
import shortid from 'shortid';
import classnames from 'classnames';
import Mousetrap from 'mousetrap';

// Directives List
import SplitColumn from 'components/DataPrep/Directives/SplitColumn';
import ParseDirective from 'components/DataPrep/Directives/Parse';
import FillNullOrEmpty from 'components/DataPrep/Directives/FillNullOrEmpty';
import DropColumn from 'components/DataPrep/Directives/DropColumn';
import FilterDirective from 'components/DataPrep/Directives/Filter';

require('./ColumnActionsDropdown.scss');

export default class ColumnActionsDropdown extends Component {
  constructor(props) {
    super(props);

    this.state = {
      dropdownOpen: false,
      open: null
    };

    this.toggleDropdown = this.toggleDropdown.bind(this);

    this.directives = [
      {
        id: shortid.generate(),
        tag: FilterDirective
      },
      {
        id: shortid.generate(),
        tag: DropColumn
      },
      {
        id: shortid.generate(),
        tag: FillNullOrEmpty
      },
      {
        id: shortid.generate(),
        tag: SplitColumn
      },
      {
        id: shortid.generate(),
        tag: ParseDirective
      }
    ];
  }

  componentWillMount() {
    this.dropdownId = shortid.generate();
  }

  componentWillUnmount() {
    if (this.documentClick$ && this.documentClick$.dispose) {
      this.documentClick$.dispose();
    }
    Mousetrap.unbind('esc');
  }

  toggleDropdown() {
    let newState = !this.state.dropdownOpen;

    this.setState({
      dropdownOpen: newState,
      open: null
    });

    if (newState) {
      let element = document.getElementById('app-container');

      this.documentClick$ = Rx.Observable.fromEvent(element, 'click')
        .subscribe((e) => {
          if (isDescendant(this.popover, e.target) || !this.state.dropdownOpen) {
            return;
          }

          this.toggleDropdown();
        });

      Mousetrap.bind('esc', this.toggleDropdown);
    } else {
      this.documentClick$.dispose();
      Mousetrap.unbind('esc');
    }
  }

  directiveClick(directive) {
    let open = directive === this.state.open ? null : directive;

    this.setState({ open });
  }

  renderMenu() {
    let tableContainer = document.getElementById('dataprep-table-id');

    const tetherOption = {
      attachment: 'top right',
      targetAttachment: 'bottom left',
      constraints: [
        {
          to: tableContainer,
          attachment: 'none together'
        }
      ]
    };

    return (
      <Popover
        placement="bottom right"
        isOpen={this.state.dropdownOpen}
        target={`dataprep-action-${this.dropdownId}`}
        className="dataprep-columns-action-dropdown"
        tether={tetherOption}
      >
        <PopoverContent>
          <div>
            {
              this.directives.map((directive) => {
                let Tag = directive.tag;

                return (
                  <div
                    key={directive.id}
                    onClick={this.directiveClick.bind(this, directive.id)}
                  >
                    <Tag
                      column={this.props.column}
                      onComplete={this.toggleDropdown}
                      isOpen={this.state.open === directive.id}
                      close={this.directiveClick.bind(this, null)}
                    />
                  </div>
                );
              })
            }
          </div>
        </PopoverContent>
      </Popover>
    );
  }

  render() {
    return (
      <span
        className="column-actions-dropdown-container"
        ref={(ref) => this.popover = ref}
      >
        <span
          className={classnames('fa fa-caret-down', {
            'expanded': this.state.dropdownOpen
          })}
          onClick={this.toggleDropdown}
          id={`dataprep-action-${this.dropdownId}`}
        />

        {this.renderMenu()}

      </span>
    );
  }
}

ColumnActionsDropdown.propTypes = {
  column: PropTypes.string
};
