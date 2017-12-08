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
import {isDescendant} from 'services/helpers';
import {Popover, PopoverContent} from 'reactstrap';
import {Observable} from 'rxjs/Observable';
import shortid from 'shortid';
import classnames from 'classnames';
import Mousetrap from 'mousetrap';
import isEqual from 'lodash/isEqual';
import DataPrepStore from 'components/DataPrep/store';
import ScrollableList from 'components/ScrollableList';

// Directives List
import ParseDirective from 'components/DataPrep/Directives/Parse';
import FillNullOrEmpty from 'components/DataPrep/Directives/FillNullOrEmpty';
import DropColumnDirective from 'components/DataPrep/Directives/DropColumn';
import KeepColumnDirective from 'components/DataPrep/Directives/KeepColumn';
import SwapColumnsDirective from 'components/DataPrep/Directives/SwapColumns';
import MergeColumnsDirective from 'components/DataPrep/Directives/MergeColumns';
import FilterDirective from 'components/DataPrep/Directives/Filter';
import FindAndReplaceDirective from 'components/DataPrep/Directives/FindAndReplace';
import CopyColumnDirective from 'components/DataPrep/Directives/CopyColumn';
import ExtractFields from 'components/DataPrep/Directives/ExtractFields';
import Format from 'components/DataPrep/Directives/Format';
import Calculate from 'components/DataPrep/Directives/Calculate';
import Explode from 'components/DataPrep/Directives/Explode';
import MaskData from 'components/DataPrep/Directives/MaskData';
import EncodeDecode from 'components/DataPrep/Directives/EncodeDecode';
import Decode from 'components/DataPrep/Directives/Decode';
import SetCharacterEncoding from 'components/DataPrep/Directives/SetCharacterEncoding';
import MarkAsError from 'components/DataPrep/Directives/MarkAsError';
import CustomTransform from 'components/DataPrep/Directives/CustomTransform';
import DefineVariableDirective from 'components/DataPrep/Directives/DefineVariable';
import SetCounterDirective from 'components/DataPrep/Directives/SetCounter';
import ChangeDataTypeDirective from 'components/DataPrep/Directives/ChangeDataType';

import ee from 'event-emitter';

require('./ColumnActionsDropdown.scss');

export default class ColumnActionsDropdown extends Component {
  constructor(props) {
    super(props);

    this.state = {
      dropdownOpen: false,
      open: null,
      selectedHeaders: DataPrepStore.getState().dataprep.selectedHeaders
    };
    this.unmounted = false;
    this.toggleDropdown = this.toggleDropdown.bind(this);


    /*
      requiredColCount attribute refers to the number of selected columns a directive needs for it to work
    e.g. MergeColumnDirective and SwapColumnDirective work when and only when the number of
    selected columns is 2.
      The possible values are:
    - 0: Works with any number of selected columns.
    - 1: Works only when there is 1 selected column.
    - 2: Works only when there are 2 selected columns.

    */
    this.directives = [
      {
        id: shortid.generate(),
        tag: ParseDirective,
        requiredColCount: 1
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: SetCharacterEncoding,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: ChangeDataTypeDirective,
        requiredColCount: 1
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: Format,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: Calculate,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: CustomTransform,
        requiredColCount: 1
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: FilterDirective,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: MarkAsError
      },
      {
        id: shortid.generate(),
        tag: FindAndReplaceDirective,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: FillNullOrEmpty,
        requiredColCount: 1
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: CopyColumnDirective,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: DropColumnDirective,
        requiredColCount: 0
      },
      {
        id: shortid.generate(),
        tag: KeepColumnDirective,
        requiredColCount: 0
      },
      {
        id: shortid.generate(),
        tag: MergeColumnsDirective,
        requiredColCount: 2
      },
      {
        id: shortid.generate(),
        tag: SwapColumnsDirective,
        requiredColCount: 2
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: ExtractFields,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: Explode,
        requiredColCount: 0
      },
      {
        id: shortid.generate(),
        tag: DefineVariableDirective,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: SetCounterDirective,
        requiredColCount: 1
      },
      {
        tag: 'divider'
      },
      {
        id: shortid.generate(),
        tag: MaskData
      },
      {
        id: shortid.generate(),
        tag: EncodeDecode,
        requiredColCount: 1
      },
      {
        id: shortid.generate(),
        tag: Decode,
        requiredColCount: 1
      }
    ];
    this.eventEmitter = ee(ee);
    this.eventEmitter.on('CLOSE_POPOVER', this.toggleDropdown.bind(this, false));
    this.dropdownId = shortid.generate();
  }

  componentDidMount() {
    this.singleWorkspaceMode = DataPrepStore.getState().dataprep.singleWorkspaceMode;

    this.sub = DataPrepStore.subscribe(() => {
      let newState = DataPrepStore.getState().dataprep;
      if (!isEqual(this.state.selectedHeaders, newState.selectedHeaders)) {
        this.setState({
          selectedHeaders: newState.selectedHeaders
        });
      }
    });
  }

  componentWillUnmount() {
    this.eventEmitter.off('CLOSE_POPOVER', this.toggleDropdown.bind(this, false));
    if (this.documentClick$ && this.documentClick$.unsubscribe) {
      this.documentClick$.unsubscribe();
    }
    this.sub();
    Mousetrap.unbind('esc');
    this.unmounted = true;
  }

  toggleDropdown(toggleState) {
    // Since this will get triggered on all the columns we could avoid setting state if its already the same.
    if (this.unmounted || toggleState === this.state.dropdownOpen) {
      return;
    }
    let newState = typeof toggleState === 'boolean' ? toggleState : !this.state.dropdownOpen;

    this.setState({
      dropdownOpen: newState,
      open: null
    });

    this.props.dropdownOpened(this.props.column, newState);

    if (newState) {
      let element = document.getElementById('app-container');
      if (!element && this.singleWorkspaceMode) {
        element = document.getElementsByClassName('wrangler-modal')[0];
      }
      this.documentClick$ = Observable.fromEvent(element, 'click')
        .subscribe((e) => {
          if (isDescendant(this.popover, e.target) || !this.state.dropdownOpen) {
            return;
          }

          this.toggleDropdown();
        });

      Mousetrap.bind('esc', this.toggleDropdown);
    } else {
      if (this.documentClick$) {
        this.documentClick$.unsubscribe();
      }
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
      classPrefix: 'column_actions_dropdown',
      constraints: [
        {
          to: tableContainer,
          attachment: 'none together'
        }
      ]
    };

    return (
      <Popover
        placement="bottom left"
        isOpen={this.state.dropdownOpen}
        target={`dataprep-action-${this.dropdownId}`}
        className="dataprep-columns-action-dropdown"
        tether={tetherOption}
      >
        <PopoverContent>
          <ScrollableList target={`dataprep-action-${this.dropdownId}`}>
              {
                this.directives.map((directive, index) => {
                  if (directive.tag === 'divider') {
                    return (
                      <div className="column-action-divider" key={index}>
                        <hr />
                      </div>
                    );
                  }
                  let Tag = directive.tag;
                  let disabled = false;
                  let column = this.props.column;

                  if (this.state.selectedHeaders.indexOf(this.props.column) !== -1) {
                    column = this.state.selectedHeaders;

                    if (this.state.selectedHeaders.length !== directive.requiredColCount && directive.requiredColCount !== 0) {
                      disabled = true;
                    }
                  } else if (directive.requiredColCount === 2) {
                    disabled = true;
                  }
                return (
                  <div
                    key={directive.id}
                    onClick={!disabled && this.directiveClick.bind(this, directive.id)}
                    className={classnames({'disabled': disabled})}
                  >
                    <Tag
                      column={column}
                      onComplete={this.toggleDropdown.bind(this, false)}
                      isOpen={this.state.open === directive.id}
                      isDisabled={disabled}
                      close={this.directiveClick.bind(this, null)}
                    />
                  </div>
                );
              })
            }
          </ScrollableList>
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
  column: PropTypes.string,
  dropdownOpened: PropTypes.func
};
