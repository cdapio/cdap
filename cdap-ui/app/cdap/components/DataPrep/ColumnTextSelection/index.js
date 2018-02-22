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
import DataPrepStore from 'components/DataPrep/store';
import classnames from 'classnames';
import uuidV4 from 'uuid/v4';
import Rx from 'rx';
import intersection from 'lodash/intersection';

require('../DataPrepTable/DataPrepTable.scss');
const CELLHIGHLIGHTCLASSNAME = 'cl-highlight';

export default class ColumnTextSelection extends Component {
  constructor(props) {
    super(props);
    this.state = {
      textSelectionRange: {start: null, end: null, index: null},
      showPopover: false,
    };

    this.newColName = null;
    this.mouseDownHandler = this.mouseDownHandler.bind(this);
    this.togglePopover = this.togglePopover.bind(this);
    this.mouseUpHandler = this.mouseUpHandler.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleColNameChange = this.handleColNameChange.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }
  componentDidMount() {
    this.documentClick$ = Rx.DOM.fromEvent(document.body, 'click', false)
      .subscribe((e) => {
        let matchingClass = intersection(e.target.className.split(' '), this.props.classNamesToExclude.concat([CELLHIGHLIGHTCLASSNAME]));
        if (
          !matchingClass.length &&
          ['TR', 'TBODY'].indexOf(e.target.nodeName) === -1
        ) {
          if (this.props.onClose) {
            this.props.onClose();
          }
        }
      });
    let highlightedHeader = document.getElementById('highlighted-header');
    if (highlightedHeader) {
      highlightedHeader.scrollIntoView();
    }
  }
  componentDidUpdate() {
    if (this.tetherRef) {
      this.tetherRef.position();
    }
  }
  componentWillUnmount() {
    if (this.documentClick$) {
      this.documentClick$.dispose();
    }
  }
  applyDirective() {
    this.onApply(this.state.textSelectionRange);
  }
  handleColNameChange(value, isChanged, keyCode) {
    this.newColName = value;
    if (keyCode === 13) {
      this.applyDirective();
    }
  }
  togglePopover() {
    if (this.state.showPopover) {
      this.setState({
        showPopover: false,
        textSelectionRange: {start: null, end: null, index: null},
      });
      this.newColName = null;
      this.props.togglePopover(false);
      return;
    }
  }
  mouseDownHandler() {
    this.textSelection = true;
    this.togglePopover();
  }
  mouseUpHandler(head, index) {
    let currentSelection = window.getSelection().toString();
    let startRange, endRange;

    if (this.textSelection && currentSelection.length) {
      startRange = window.getSelection().getRangeAt(0).startOffset;
      endRange = window.getSelection().getRangeAt(0).endOffset;
      this.textSelection = false;
      let textSelectionRange = {
        start: startRange,
        end: endRange,
        index
      };
      this.setState({
        showPopover: true,
        textSelectionRange
      });
      this.props.onSelect({
        textSelectionRange,
        rowNumber: index
      });
      this.props.togglePopover(true);
      this.newColName = this.props.columns[0] + '_copy';
    } else {
      if (this.state.showPopover) {
        this.props.onSelect({textSelectionRange: null, showPopover:true, rowNumber: null});
        this.togglePopover();
      }
    }
  }
  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  render() {
    let {data, headers} = DataPrepStore.getState().dataprep;
    let column = this.props.columns[0];

    const renderTableCell = (row, index, head, highlightColumn) => {
      if (head !== highlightColumn) {
        return (
          <td
            key={uuidV4()}
            className="gray-out"
          >
            <div>
              {row[head]}
            </div>
          </td>
        );
      }
      return (
        <td
          key={uuidV4()}
          className={CELLHIGHLIGHTCLASSNAME}
          onMouseDown={this.mouseDownHandler}
          onMouseUp={this.mouseUpHandler.bind(this, head, index)}
        >
          <div
            className={CELLHIGHLIGHTCLASSNAME}
          >
            {
              index === this.state.textSelectionRange.index ?
                (
                  <span>
                    <span>
                      {
                        row[head].slice(0, this.state.textSelectionRange.start)
                      }
                    </span>
                    <span id={`highlight-cell-${index}`}>
                      {
                        row[head].slice(this.state.textSelectionRange.start, this.state.textSelectionRange.end)
                      }
                    </span>
                    <span>
                      {
                        row[head].slice(this.state.textSelectionRange.end)
                      }
                    </span>
                  </span>
                )
              :
                row[head]
            }
          </div>
        </td>
      );
    };
    const renderTableHeader = (head, index) => {
      if (head !== column) {
        return (
          <th key={index} className="gray-out">
            <div>
              {head}
            </div>
          </th>
        );
      }

      return (
        <th key={index} id="highlighted-header">
          <div>
            {head}
          </div>
        </th>
      );
    };
    return (
      <div
        id="cut-directive"
        className={classnames("dataprep-table", this.props.className)}
      >
        <table className="table table-bordered">
          <colgroup>
            <col />
            {
              headers.map((head, i) => {
                return (
                  <col
                    key={i}
                    className={classnames({
                      "highlight-column": head === column
                    })}
                  />
                );
              })
            }
          </colgroup>
          <thead className="thead-inverse">
            <tr>
              <th />
              {
                headers.map((head, i) => {
                  return renderTableHeader(head, i);
                })
              }
            </tr>
          </thead>
          <tbody>
              {
                data.map((row, i) => {
                  return (
                    <tr key={i}>
                      <td>{i}</td>
                      {
                        headers.map((head) => {
                          return renderTableCell(row, i, head, column);
                        })
                      }
                    </tr>
                  );
                })
              }
          </tbody>
        </table>
        {this.props.renderPopover()}
      </div>
    );
  }
}

ColumnTextSelection.defaultProps = {
  classNamesToExclude: []
};

ColumnTextSelection.propTypes = {
  columns: PropTypes.arrayOf(PropTypes.string),
  onClose: PropTypes.func,
  renderPopover: PropTypes.func.isRequired,
  onApply: PropTypes.func.isRequired,
  onSelect: PropTypes.func.isRequired,
  togglePopover: PropTypes.func.isRequired,
  className: PropTypes.string,
  classNamesToExclude: PropTypes.arrayOf(PropTypes.string)
};
