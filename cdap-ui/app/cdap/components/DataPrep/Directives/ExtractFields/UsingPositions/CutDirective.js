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
import DataPrepStore from 'components/DataPrep/store';
import classnames from 'classnames';
import shortid from 'shortid';
import Rx from 'rx';
import { Popover, PopoverTitle, PopoverContent } from 'reactstrap';
import isNil from 'lodash/isNil';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import T from 'i18n-react';
import TextboxOnValium from 'components/TextboxOnValium';

require('../../../DataPrepTable/DataPrepTable.scss');
require('./CutDirective.scss');
const CELLHIGHLIGHTCLASSNAME = 'cl-highlight';
const POPOVERTHETHERCLASSNAME = 'highlight-popover';
const PREFIX = `features.DataPrep.Directives.CutDirective`;

export default class CutDirective extends Component {
  constructor(props) {
    super(props);
    this.state = {
      columnDimension: {},
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
  componentWillMount() {
    if (this.props.columns.length) {
      let column = this.props.columns[0];
      let ele = document.getElementById(`column-${column}`);
      this.setState({
        columnDimension: ele.getBoundingClientRect()
      });
    }
  }
  componentDidMount() {
    this.documentClick$ = Rx.DOM.fromEvent(document.body, 'click', false)
      .subscribe((e) => {
        if (
          e.target.className.indexOf(CELLHIGHLIGHTCLASSNAME) === -1 &&
          e.target.className.indexOf(`${POPOVERTHETHERCLASSNAME}-element`) === -1 &&
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
      this.setState({
        showPopover: true,
        textSelectionRange: {
          start: startRange,
          end: endRange,
          index
        },
      });
      this.newColName = this.props.columns[0] + '_copy';
    } else {
      if (this.state.showPopover) {
        this.togglePopover();
      }
    }
  }
  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }
  renderPopover() {
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
    let {data, headers} = DataPrepStore.getState().dataprep;
    let column = this.props.columns[0];

    const renderTableCell = (row, index, head, highlightColumn) => {
      if (head !== highlightColumn) {
        return (
          <td
            key={shortid.generate()}
            className="gray-out"
          >
            <div className="gray-out">
              {row[head]}
            </div>
          </td>
        );
      }
      return (
        <td
          key={shortid.generate()}
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
    const renderTableHeader = (head) => {
      if (head !== column) {
        return (
          <th className="gray-out">
            <div className="gray-out">
              {head}
            </div>
          </th>
        );
      }

      return (
        <th id="highlighted-header">
          <div>
            {head}
          </div>
        </th>
      );
    };
    return (
      <div
        id="cut-directive"
        className="cut-directive dataprep-table"
      >
        <table className="table table-bordered">
          <colgroup>
            <col />
            {
              headers.map(head => {
                return (
                  <col className={classnames({
                    "highlight-column": head === column
                  })} />
                );
              })
            }
          </colgroup>
          <thead className="thead-inverse">
            <tr>
              <th />
              {
                headers.map( head => {
                  return renderTableHeader(head);
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
        {this.renderPopover()}
      </div>
    );
  }
}

CutDirective.propTypes = {
  columns: PropTypes.arrayOf(PropTypes.string),
  onClose: PropTypes.func
};
