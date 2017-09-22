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
import ColumnTextSelection from 'components/DataPrep/ColumnTextSelection';
import { Popover, PopoverTitle, PopoverContent } from 'reactstrap';
import T from 'i18n-react';
import DataPrepStore from 'components/DataPrep/store';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import Mousetrap from 'mousetrap';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

const POPOVERTHETHERCLASSNAME = 'highlight-popover';
const CELLHIGHLIGHTCLASSNAME = 'cl-highlight';
const PREFIX = `features.DataPrep.Directives.MaskSelection`;

export default class MaskSelection extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showPopover: false,
      textSelectionRange: null,
      rowNumber: null
    };
    this.renderPopover = this.renderPopover.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
    this.onTextSelection = this.onTextSelection.bind(this);
    this.togglePopover = this.togglePopover.bind(this);
  }

  componentDidMount() {
    Mousetrap.bind('enter', this.applyDirective);
  }
  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  getPattern() {
    let {start, end} = this.state.textSelectionRange;
    const getMaskPattern = (N) => Array.apply(null, {length: N}).map(() => 'x').join('');
    const getAllowPattern = (N) => Array.apply(null, {length: N}).map(() => '#').join('');
    let {data} = DataPrepStore.getState().dataprep;
    let length = data[this.state.rowNumber][this.props.columns].length;
    if (start === 0) {
      return getMaskPattern(end) + getAllowPattern(length - end);
    }
    return getAllowPattern(start) + getMaskPattern(end - start) + getAllowPattern(length - end);
  }
  applyDirective() {
    let pattern = this.getPattern();
    let directive = [`mask-number ${this.props.columns.toString()} ${pattern}`];
    execute(directive)
      .subscribe(
        () => {
          this.props.onClose();
        },
        (err) => {
          console.log('error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }
  onTextSelection({textSelectionRange, rowNumber}) {
    this.setState({
      textSelectionRange,
      rowNumber
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
          <p className={`${CELLHIGHLIGHTCLASSNAME}`}>
            {T.translate(`${PREFIX}.description`)}
          </p>
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
MaskSelection.propTypes = {
  onClose: PropTypes.func,
  columns: PropTypes.arrayOf(PropTypes.string)
};
