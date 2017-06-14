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

import React, {Component, PropTypes} from 'react';
import ColumnTextSelection from 'components/DataPrep/ColumnTextSelection';
import { Popover, PopoverTitle, PopoverContent } from 'reactstrap';
import T from 'i18n-react';
import TextboxOnValium from 'components/TextboxOnValium';
import classnames from 'classnames';
import isNil from 'lodash/isNil';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import DataPrepStore from 'components/DataPrep/store';

require('./CutDirective.scss');

const CELLHIGHLIGHTCLASSNAME = 'cl-highlight';
const POPOVERTHETHERCLASSNAME = 'highlight-popover';
const PREFIX = `features.DataPrep.Directives.CutDirective`;

export default class CutDirective extends Component {
  constructor(props) {
    super(props);
    this.state = {
      textSelectionRange: {start: null, end: null, index: null},
      showPopover: false
    };
    this.newColName = null;
    this.onTextSelection = this.onTextSelection.bind(this);
    this.renderPopover = this.renderPopover.bind(this);
    this.togglePopover = this.togglePopover.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
    this.handleColNameChange = this.handleColNameChange.bind(this);
  }

  handleColNameChange(value, isChanged, keyCode) {
    this.newColName = value;
    if (keyCode === 13) {
      this.applyDirective();
    }
  }

  applyDirective() {
    let {start, end} = this.state.textSelectionRange;
    if (!isNil(start) && !isNil(end)) {
      let directive = `cut-character ${this.props.columns[0]} ${this.newColName} ${start + 1}-${end}`;
      execute([directive])
        .subscribe(() => {
          this.props.onClose();
        }, (err) => {
          console.log('error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        });
    }
  }

  onTextSelection({textSelectionRange}) {
    this.setState({
      textSelectionRange
    });
  }
  togglePopover(showPopover) {
    this.setState({
      showPopover
    });
  }
  renderPopover() {
    if (!this.state.showPopover) {
      return null;
    }
    let tetherConfig = {
      classPrefix: POPOVERTHETHERCLASSNAME,
      attachment: 'top right',
      targetAttachment: 'bottom left',
      constraints: [
        {
          to: 'scrollParent',
          attachment: 'together'
        }
      ]
    };
    let {start, end} = this.state.textSelectionRange;
    return (
      <Popover
        placement="bottom left"
        className="cut-directive-popover"
        isOpen={this.state.showPopover}
        target={`highlight-cell-${this.state.textSelectionRange.index}`}
        toggle={this.togglePopover}
        tether={tetherConfig}
        tetherRef={(ref) => this.tetherRef = ref}
      >
        <PopoverTitle className={CELLHIGHLIGHTCLASSNAME}>{T.translate(`${PREFIX}.popoverTitle`)}</PopoverTitle>
        <PopoverContent
          className={CELLHIGHLIGHTCLASSNAME}
          onClick={this.preventPropagation}
        >
          <span className={CELLHIGHLIGHTCLASSNAME}>
            {T.translate(`${PREFIX}.extractDescription`, {range: `${start + 1}-${end}`})}
          </span>
          <div className={classnames("col-input-container", CELLHIGHLIGHTCLASSNAME)}>
            <strong className={CELLHIGHLIGHTCLASSNAME}>{T.translate(`${PREFIX}.inputLabel`)}</strong>
            <TextboxOnValium
              className={classnames("form-control mousetrap", CELLHIGHLIGHTCLASSNAME)}
              onChange={this.handleColNameChange}
              value={this.newColName}
            />
          </div>
          <div
            className={`btn btn-primary ${CELLHIGHLIGHTCLASSNAME}`}
            onClick={this.applyDirective}
          >
            {T.translate('features.DataPrep.Directives.apply')}
          </div>
          <div
            className={`btn ${CELLHIGHLIGHTCLASSNAME}`}
            onClick={this.props.onClose}
          >
            {T.translate(`${PREFIX}.cancelBtnLabel`)}
          </div>
        </PopoverContent>
      </Popover>
    );
  }

  render() {
    return (
      <ColumnTextSelection
        className="cut-directive"
        renderPopover={this.renderPopover}
        onApply={this.applyDirective}
        onClose={this.props.onClose}
        columns={this.props.columns}
        classNamesToExclude={[POPOVERTHETHERCLASSNAME]}
        onSelect={this.onTextSelection}
        togglePopover={this.togglePopover}
      />
    );
  }
}

CutDirective.propTypes = {
  onClose: PropTypes.func,
  columns: PropTypes.arrayOf(PropTypes.string)
};
