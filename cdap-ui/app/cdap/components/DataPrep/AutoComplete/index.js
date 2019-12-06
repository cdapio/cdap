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
import MyDataPrepApi from 'api/dataprep';
import Fuse from 'fuse.js';
import uuidV4 from 'uuid/v4';
import reverse from 'lodash/reverse';
import Mousetrap from 'mousetrap';
import classnames from 'classnames';
import NamespaceStore from 'services/NamespaceStore';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

require('./AutoComplete.scss');

export default class DataPrepAutoComplete extends Component {
  constructor(props) {
    super(props);

    this.state = {
      activeResults: [],
      input: '',
      matched: false,
      activeSelectionIndex: null,
    };

    this.eventEmitter = ee(ee);

    this.getUsage = this.getUsage.bind(this);

    this.eventEmitter.on(globalEvents.DIRECTIVEUPLOAD, this.getUsage);
  }

  componentWillMount() {
    this.getUsage();
  }

  componentDidMount() {
    let directiveInput = document.getElementById('directive-input');
    this.mousetrap = new Mousetrap(directiveInput);

    this.mousetrap.bind('esc', this.props.toggle);
    this.mousetrap.bind('up', this.handleUpArrow.bind(this));
    this.mousetrap.bind('down', this.handleDownArrow.bind(this));
    this.mousetrap.bind('enter', this.handleEnterKey.bind(this));
    this.mousetrap.bind('tab', this.handleTabKey.bind(this));
  }

  componentWillUnmount() {
    this.mousetrap.unbind('esc');
    this.mousetrap.unbind('up');
    this.mousetrap.unbind('down');
    this.mousetrap.unbind('enter');
    this.mousetrap.unbind('tab');
    this.eventEmitter.off(globalEvents.DIRECTIVEUPLOAD, this.getUsage);
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.input !== this.state.input) {
      this.searchMatch(nextProps.input);
    }
  }

  getUsage() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.getUsage({ context: namespace }).subscribe((res) => {
      const fuseOptions = {
        includeScore: true,
        includeMatches: true,
        caseSensitive: false,
        threshold: 0,
        shouldSort: true,
        location: 0,
        distance: 100,
        minMatchCharLength: 1,
        maxPatternLength: 32,
        keys: ['directive'],
      };

      this.fuse = new Fuse(res.values, fuseOptions);
    });
  }

  handleRowClick(row) {
    if (typeof this.props.onRowClick !== 'function') {
      return;
    }

    let eventObject = {
      target: { value: `${row.item.directive} ` },
    };

    this.props.onRowClick(eventObject);
    this.props.inputRef.focus();
  }

  handleUpArrow(e) {
    if (e.preventDefault) {
      e.preventDefault();
    } else {
      e.returnValue = false;
    }
    if (this.state.activeSelectionIndex === 0) {
      return;
    }

    this.setState({ activeSelectionIndex: this.state.activeSelectionIndex - 1 });
  }

  handleDownArrow(e) {
    if (e.preventDefault) {
      e.preventDefault();
    } else {
      e.returnValue = false;
    }
    if (this.state.activeSelectionIndex === this.state.activeResults.length - 1) {
      return;
    }

    this.setState({ activeSelectionIndex: this.state.activeSelectionIndex + 1 });
  }

  handleEnterKey() {
    if (this.state.input.length === 0) {
      return;
    }

    let selectedDirective = this.state.activeResults[this.state.activeSelectionIndex];

    let inputSplit = this.state.input.split(' '),
      directiveSplit = selectedDirective ? selectedDirective.item.directive.split(' ') : [];

    let splitLengthCheck = inputSplit.length < directiveSplit.length,
      stringLengthCheck =
        selectedDirective && this.state.input.length <= selectedDirective.item.directive.length;

    if (selectedDirective && (splitLengthCheck || stringLengthCheck)) {
      this.handleRowClick(this.state.activeResults[this.state.activeSelectionIndex]);
    } else {
      this.props.execute([this.state.input]);
    }
  }

  handleTabKey(e) {
    if (this.state.input.length === 0 || this.state.input.split(' ').length !== 1) {
      return;
    }

    if (e.preventDefault) {
      e.preventDefault();
    } else {
      e.returnValue = false;
    }

    this.handleEnterKey();
  }

  searchMatch(query) {
    let results = [];
    let input = query;
    let spaceIndex = input.indexOf(' ');
    if (spaceIndex !== -1) {
      input = input.slice(0, spaceIndex);
    }

    // Currently only showing 3 matches. Future improvement will be to allow
    // user to scroll through all the matches
    if (this.fuse && input.length > 0) {
      results = this.fuse
        .search(input)
        .slice(0, 3)
        .filter((row, index) => {
          if (spaceIndex === -1) {
            return true;
          }

          return row.score === 0 && index === 0;
        })
        .map((row) => {
          row.uniqueId = uuidV4();
          return row;
        });

      reverse(results);
    }

    this.setState({
      activeResults: results,
      input: query,
      matched: spaceIndex !== -1,
      activeSelectionIndex: results.length - 1,
    });
  }

  render() {
    if (!this.props.isOpen || this.state.activeResults.length === 0) {
      return null;
    }

    return (
      <div
        className={classnames('dataprep-auto-complete-container', {
          'has-error': this.props.hasError,
        })}
      >
        {this.state.activeResults.map((row, index) => {
          return (
            <div
              className={classnames('result-row', {
                active: index === this.state.activeSelectionIndex,
              })}
              key={row.uniqueId}
              onClick={this.handleRowClick.bind(this, row)}
            >
              <div className="directive-title">
                <strong>{row.item.directive}</strong>
              </div>
              <div className="directive-description">{row.item.description}</div>

              {this.state.matchedmatched || this.state.activeResults.length === 1 ? (
                <div className="directive-usage">
                  <span>Usage: </span>
                  <pre>{row.item.usage}</pre>
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    );
  }
}

DataPrepAutoComplete.propTypes = {
  isOpen: PropTypes.bool,
  toggle: PropTypes.func,
  input: PropTypes.string,
  onRowClick: PropTypes.func,
  inputRef: PropTypes.any,
  hasError: PropTypes.any,
  execute: PropTypes.func,
};
