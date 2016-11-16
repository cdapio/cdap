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

import React, { Component, PropTypes } from 'react';
import Rx from 'rx';
import {isDescendant} from 'services/helpers';
import DropAction from 'components/Wrangler/ColumnActions/DropAction';
import SplitAction from 'components/Wrangler/ColumnActions/SplitAction';
import UpperCaseAction from 'components/Wrangler/ColumnActions/UpperCaseAction';
import LowerCaseAction from 'components/Wrangler/ColumnActions/LowerCaseAction';
import TitleCaseAction from 'components/Wrangler/ColumnActions/TitleCaseAction';
import SubstringAction from 'components/Wrangler/ColumnActions/SubstringAction';
import MergeAction from 'components/Wrangler/ColumnActions/MergeAction';
import RenameAction from 'components/Wrangler/ColumnActions/RenameAction';

require('./ColumnActionsDropdown.less');

export default class ColumnActionsDropdown extends Component {
  constructor(props) {
    super(props);

    this.state = {
      isOpen: false
    };

    this.toggle = this.toggle.bind(this);
  }

  toggle() {
    let setState = !this.state.isOpen;

    this.setState({isOpen: setState});

    if (setState) {
      let appContainer = document.getElementById('app-container');
      this.documentClick$ = Rx.Observable.fromEvent(appContainer, 'click')
        .subscribe((e) => {
          if (isDescendant(this.popover, e.target) || !this.state.isOpen) {
            return;
          }

          this.toggle();
        });
    } else {
      this.documentClick$.dispose();
    }
  }

  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.dispose();
    }
  }

  renderPopover() {
    if (!this.state.isOpen) { return null; }

    return (
      <div
        className="actions-popover"
        ref={(ref) => this.popover = ref}
      >
        <div className="actions-list">
          <DropAction column={this.props.column} />
          <SplitAction column={this.props.column} />
          <MergeAction column={this.props.column} />
          <SubstringAction column={this.props.column} />
          <UpperCaseAction column={this.props.column} />
          <LowerCaseAction column={this.props.column} />
          <TitleCaseAction column={this.props.column} />
          <RenameAction column={this.props.column} />
        </div>
      </div>
    );
  }

  render() {
    const columnId = `column-${this.props.column}`;

    return (
      <span className="column-actions-dropdown">
        <span
          className="fa fa-bolt"
          id={columnId}
          onClick={this.toggle}
        />

        {this.renderPopover()}
      </span>
    );
  }
}

ColumnActionsDropdown.propTypes = {
  column: PropTypes.string
};
