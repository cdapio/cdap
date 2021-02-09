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
import { isDescendant } from 'services/helpers';
import { Popover, PopoverBody, UncontrolledTooltip } from 'reactstrap';
import { Observable } from 'rxjs/Observable';
import uuidV4 from 'uuid/v4';
import classnames from 'classnames';
import Mousetrap from 'mousetrap';
import isEqual from 'lodash/isEqual';
import DataPrepStore from 'components/DataPrep/store';
import ScrollableList from 'components/ScrollableList';
import { Theme } from 'services/ThemeHelper';

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
import Hash from 'components/DataPrep/Directives/Hash';
import SetCharacterEncoding from 'components/DataPrep/Directives/SetCharacterEncoding';
import MarkAsError from 'components/DataPrep/Directives/MarkAsError';
import CustomTransform from 'components/DataPrep/Directives/CustomTransform';
import DefineVariableDirective from 'components/DataPrep/Directives/DefineVariable';
import SetCounterDirective from 'components/DataPrep/Directives/SetCounter';
import ChangeDataTypeDirective from 'components/DataPrep/Directives/ChangeDataType';
import MapToTargetDirective from 'components/DataPrep/Directives/MapToTarget';

import ee from 'event-emitter';

require('./ColumnActionsDropdown.scss');

export default class ColumnActionsDropdown extends Component {
  constructor(props) {
    super(props);

    this.state = {
      dropdownOpen: false,
      open: null,
      selectedHeaders: DataPrepStore.getState().dataprep.selectedHeaders,
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
    this.directives = [];
    this.directives.push(
      {
        id: uuidV4(),
        tag: ParseDirective,
        requiredColCount: 1,
      },
      {
        tag: 'divider',
      }
    );
    if (Theme.showWranglerDataModel && Theme.wranglerDataModelUrl) {
      this.directives.push(
        {
          id: uuidV4(),
          tag: MapToTargetDirective,
          requiredColCount: 1,
        },
        {
          tag: 'divider',
        }
      );
    }
    this.directives.push(
      {
        id: uuidV4(),
        tag: SetCharacterEncoding,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: ChangeDataTypeDirective,
        requiredColCount: 1,
      },
      {
        tag: 'divider',
      },
      {
        id: uuidV4(),
        tag: Format,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: Calculate,
        requiredColCount: 0,
      },
      {
        id: uuidV4(),
        tag: CustomTransform,
        requiredColCount: 1,
      },
      {
        tag: 'divider',
      },
      {
        id: uuidV4(),
        tag: FilterDirective,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: MarkAsError,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: FindAndReplaceDirective,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: FillNullOrEmpty,
        requiredColCount: 1,
      },
      {
        tag: 'divider',
      },
      {
        id: uuidV4(),
        tag: CopyColumnDirective,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: DropColumnDirective,
        requiredColCount: 0,
      },
      {
        id: uuidV4(),
        tag: KeepColumnDirective,
        requiredColCount: 0,
      },
      {
        id: uuidV4(),
        tag: MergeColumnsDirective,
        requiredColCount: 2,
      },
      {
        id: uuidV4(),
        tag: SwapColumnsDirective,
        requiredColCount: 2,
      },
      {
        tag: 'divider',
      },
      {
        id: uuidV4(),
        tag: ExtractFields,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: Explode,
        requiredColCount: 0,
      },
      {
        id: uuidV4(),
        tag: DefineVariableDirective,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: SetCounterDirective,
        requiredColCount: 1,
      },
      {
        tag: 'divider',
      },
      {
        id: uuidV4(),
        tag: MaskData,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: EncodeDecode,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: Decode,
        requiredColCount: 1,
      },
      {
        id: uuidV4(),
        tag: Hash,
        requiredColCount: 1,
      }
    );
    this.eventEmitter = ee(ee);
    this.eventEmitter.on('CLOSE_POPOVER', this.toggleDropdown.bind(this, false));
    this.dropdownId = uuidV4();
  }

  componentDidMount() {
    this.singleWorkspaceMode = DataPrepStore.getState().dataprep.singleWorkspaceMode;

    this.sub = DataPrepStore.subscribe(() => {
      let newState = DataPrepStore.getState().dataprep;
      if (!isEqual(this.state.selectedHeaders, newState.selectedHeaders)) {
        this.setState({
          selectedHeaders: newState.selectedHeaders,
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
      open: null,
    });

    this.props.dropdownOpened(this.props.column, newState);

    if (newState) {
      let element = document.getElementById('app-container');
      if (this.singleWorkspaceMode) {
        element = document.getElementsByClassName('wrangler-modal')[0];
      }
      this.documentClick$ = Observable.fromEvent(element, 'click').subscribe((e) => {
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
    return (
      <Popover
        placement="bottom-start"
        isOpen={this.state.dropdownOpen}
        target={`dataprep-action-${this.dropdownId}`}
        innerClassName="dataprep-columns-action-dropdown"
        fade={false}
        popperClassName="column_actions_dropdown-element"
        modifiers={{
          shift: {
            enabled: true,
            order: 800,
            fn: (data) => {
              /**
               * WTH??
               *
               * 1. With tether we used to be able to leave this calculation to tether to figure
               *    out the optimal placement for popover based on container's(dataprep table) boundaries
               * 2. Now that we have moved to popper.js thats no longer the case and we need to position
               *    the popover manually based on our requirement,
               *
               * Conditions:
               *
               *    If selection is close to the left end of the container(table)
               *      - the LEFT of reference element(down arrow) aligns to the LEFT of the popover element
               *    If selection close to the right end of the container(table)
               *      - the RIGHT of the reference element(down arrow) aligns to the RIGHT of the popover element
               *
               * This is so that we can prevent the third level menu to not hide beyond the right edges of
               * the container.
               *
               * JIRA: https://issues.cask.co/browse/CDAP-14714
               *
               */
              if (data.offsets.popper.left < data.popper.width) {
                data.offsets.popper.left = data.offsets.reference.left;
                data.popper.left = data.offsets.popper.left;
                data.placement = 'bottom-start';
                data.originalPlacement = 'bottom-start';
                return data;
              }
              if (data.offsets.reference.right < data.offsets.popper.right) {
                data.offsets.popper.left = data.offsets.reference.right - data.popper.width;
                data.popper.left = data.offsets.popper.left;
                data.placement = 'bottom-end';
                data.originalPlacement = 'bottom-end';
              }
              return data;
            },
          },
          preventOverflow: {
            enabled: true,
            boundariesElement: tableContainer,
            priority: ['left'],
          },
        }}
        hideArrow
      >
        <PopoverBody>
          <ScrollableList target={`dataprep-action-${this.dropdownId}`}>
            {this.directives.map((directive, index) => {
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

                if (
                  this.state.selectedHeaders.length !== directive.requiredColCount &&
                  directive.requiredColCount !== 0
                ) {
                  disabled = true;
                }
              } else if (directive.requiredColCount === 2) {
                disabled = true;
              }
              return (
                <div
                  key={directive.id}
                  onClick={!disabled ? this.directiveClick.bind(this, directive.id) : undefined}
                  className={classnames({ disabled: disabled })}
                >
                  <Tag
                    column={column}
                    // Needed for the destination column where the result should be saved
                    ddSelected={this.props.column}
                    onComplete={this.toggleDropdown.bind(this, false)}
                    isOpen={this.state.open === directive.id}
                    isDisabled={disabled}
                    close={this.directiveClick.bind(this, null)}
                  />
                </div>
              );
            })}
          </ScrollableList>
        </PopoverBody>
      </Popover>
    );
  }

  render() {
    return (
      <span className="column-actions-dropdown-container" ref={(ref) => (this.popover = ref)}>
        <button
          className={classnames('fa fa-caret-down', {
            expanded: this.state.dropdownOpen,
          })}
          onClick={this.toggleDropdown}
          id={`dataprep-action-${this.dropdownId}`}
          disabled={this.props.disabled}
        />
        <UncontrolledTooltip
          target={`dataprep-action-${this.dropdownId}`}
          placement="top"
          modifiers={{
            flip: {
              enabled: false,
              boundariesElement: document.querySelector('.dataprephome-wrapper'),
            },
            // FIXME (CDAP-15360): This offset is not being applied because the popover overlaps with its boundary.
            offset: {
              offset: '0 3',
            },
          }}
        >
          Column transformations
        </UncontrolledTooltip>
        {this.renderMenu()}
      </span>
    );
  }
}

ColumnActionsDropdown.propTypes = {
  column: PropTypes.string,
  dropdownOpened: PropTypes.func,
  disabled: PropTypes.bool,
};
