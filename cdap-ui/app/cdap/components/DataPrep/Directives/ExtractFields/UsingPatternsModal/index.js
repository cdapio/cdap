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
import {Modal, ModalHeader, ModalBody, ModalFooter} from 'reactstrap';
import T from 'i18n-react';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import {Input, DropdownToggle, DropdownMenu, DropdownItem} from 'reactstrap';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
const PREFIX = 'features.DataPrep.Directives.ExtractFields.UsingPatterns';
import Mousetrap from 'mousetrap';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {UncontrolledDropdown} from 'components/UncontrolledComponents';

require('./UsingPatternsModal.scss');

export default class UsingPatternsModal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      pattern: null,
      patternName: null,
      patternLabel: null,
      startAndEndPattern: {
        start: '',
        end: ''
      },
      nDigitPattern: '',
      error: '',
      showEditPatternLabel: false,
      showEditPatternTxtBox: false
    };
    this.patterns = [
      {
        label: T.translate(`${PREFIX}.selectPatternMessage`),
        value: null
      },
      {
        label: T.translate(`${PREFIX}.creditCardPattern`),
        value: `((?:\\d{4}[-\\s]?){4})`,
        patternName: 'creditcard'
      },
      {
        label: T.translate(`${PREFIX}.datePattern`),
        value: `((?:(?:\\d{4}|\\d{2})(?:(?:[.,]\\s)|[-\/.\\s])(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{1,2}))|(?:(?:(?:\\d{1,2})(?:(?:[.,]\\s)|[-\/.\\s])(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))|(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{1,2}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{4}|\\d{2})))`,
        patternName: 'date'
      },
      {
        label: T.translate(`${PREFIX}.datetimePattern`),
        value: `((?:(?:(?:\\d{4}|\\d{2})(?:(?:[.,]\\s)|[-\/.\\s])(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{1,2}))|(?:(?:(?:\\d{1,2})(?:(?:[.,]\\s)|[-\/.\\s])(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))|(?:(?:1[0-2])|(?:0?\\d)|(?:[a-zA-Z]{3}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{1,2}))(?:(?:[.,]\\s)|[-\/.\\s])(?:\\d{4}|\\d{2})))[T\\s](?:(?:(?:2[0-3])|(?:[01]?\\d))[h:\\s][0-5]\\d(?::(?:(?:[0-5]\\d)|(?:60)))?(?:\\s[aApP][mM])?(?:Z|(?:[+-](?:1[0-2])|(?:0?\\d):[0-5]\\d)|(?:\\s[[a-zA-Z]\\s]+))?))`,
        patternName: 'datetime'
      },
      {
        label: T.translate(`${PREFIX}.emailPattern`),
        value: `([a-zA-Z0-9!#$%&*+/=?^_\`'{|}~-]+@(?!.*\\.{2})[a-zA-Z0-9\\.-]+(?:\\.[a-zA-Z]{2,6})?)`,
        patternName: 'email'
      },
      {
        label: T.translate(`${PREFIX}.htmlHyperlinkPattern`),
        value: `<[aA](?:\\s+[a-zA-Z]+=".*?")*\\s+[hH][rR][eE][fF]="(.*?)"(?:\\s+[a-zA-Z]+=".*?")*>(?:.*)<\/[aA]>`,
        patternName: 'htmlhyperlink'
      },
      {
        label: T.translate(`${PREFIX}.ipv4Pattern`),
        value: `((?:(?:0|(?:25[0-5])|(?:2[0-4][1-9])|(?:1\\d\\d)|(?:[1-9]\\d?))\\.){3}(?:(?:0|(?:25[0-5])|(?:2[0-4][1-9])|(?:1\\d\\d)|(?:[1-9]\\d?))))`,
        patternName: 'ipv4'
      },
      {
        label: T.translate(`${PREFIX}.isbncodePattern`),
        value: `((?:97[89]-?)?(?:\\d-?){9}[\\dxX])`,
        patternName: 'isbncodes'
      },
      {
        label: T.translate(`${PREFIX}.macaddressPattern`),
        value: `((?:\\p{XDigit}{2}[:-]){5}(?:\\p{XDigit}{2}))`,
        patternName: 'macaddress'
      },
      {
        label: T.translate(`${PREFIX}.ndigitnumberPattern`),
        value: null,
        patternName: 'ndigitnumber',
        getRegex: (n) => `(\\d{${n}})`
      },
      {
        label: T.translate(`${PREFIX}.phoneNumberPattern`),
        value: `((?:\\+\\d{1,3}[\\s-]?)?\\(?\\d{3}\\)?[\\s-]?\\d{3}[\\s-]?\\d{4})`,
        patternName: 'phonenumber'
      },
      {
        label: T.translate(`${PREFIX}.ssnPattern`),
        value: `(\\d{3}[-\\s]?\\d{2}[-\\s]?\\d{4})`,
        patternName: 'ssn'
      },
      {
        label: T.translate(`${PREFIX}.startEndPattern`),
        value: null,
        getRegex: (start, end) => `.*${start}(.*)${end}.*`,
        patternName: 'startend'
      },
      {
        label: T.translate(`${PREFIX}.timePattern`),
        value:  `((?:(?:2[0-3])|(?:[01]?\\d))[h:\\s][0-5]\\d(?::(?:(?:[0-5]\\d)|(?:60)))?(?:\\s[aApP][mM])?(?:Z|(?:[+-](?:1[0-2])|(?:0?\\d):[0-5]\\d)|(?:\\s[[a-zA-Z]\\s]+))?)`,
        patternName: 'time'
      },
      {
        label: T.translate(`${PREFIX}.upscodePattern`),
        value: `(1Z\\s?[0-9a-zA-Z]{3}\\s?[0-9a-zA-Z]{3}\\s?[0-9a-zA-Z]{2}\\s?\\d{4}\\s?\\d{4})`,
        patternName: 'upscodes'
      },
      {
        label: T.translate(`${PREFIX}.urlPattern`),
        value: `((?:(?:http[s]?|ftp):\/)?\/?(?:[^\/\\s]+)(?:(?:\/\\w+)*\/)(?:[\\w\-\.]+[^#?\\s]+)(?:.*)?)`,
        patternName: 'url'
      },
      {
        label: T.translate(`${PREFIX}.zipCodePattern`),
        value: `[^\\d]?([0-9]{5}(?:-[0-9]{4})?)[^\\d]?`,
        patternName: 'zipcode'
      },
      {
        label: T.translate(`${PREFIX}.customPattern`),
        value: null,
        patternName: 'custom'
      }
    ];
    this.onPatternChange = this.onPatternChange.bind(this);
    this.setStartEndPattern = this.setStartEndPattern.bind(this);
    this.setCustomPattern = this.setCustomPattern.bind(this);
    this.toggleShowEditPatternTxtBox = this.toggleShowEditPatternTxtBox.bind(this);
    this.onRawPatternChange = this.onRawPatternChange.bind(this);
    this.setNDigitPattern = this.setNDigitPattern.bind(this);
    this.applyDirective = this.applyDirective.bind(this);
  }
  componentDidMount() {
    Mousetrap.bind('enter', this.applyDirective);
  }
  componentWillUnmount() {
    Mousetrap.unbind('enter');
  }

  toggleShowEditPatternTxtBox() {
    this.setState({
      showEditPatternTxtBox: !this.state.showEditPatternTxtBox
    });
  }
  onRawPatternChange(e) {
    this.setState({
      pattern: isEmpty(e.target.value) ? null : e.target.value
    });
  }
  onPatternChange(index) {
    let selectedIndex = index;
    let {value, patternName, label, getRegex = null} = this.patterns[selectedIndex];
    let showPatternLabel = [null, 'custom'].indexOf(patternName) === -1 && isNil(getRegex);
    if (selectedIndex) {
      this.setState({
        pattern: value,
        patternName,
        patternLabel: label,
        showEditPatternLabel: showPatternLabel,
        showEditPatternTxtBox: false,
        startAndEndPattern: {
          start: '',
          end: ''
        },
        nDigitPattern: '',
      });
    } else {
      this.setState({
        pattern: null,
        patternName: null,
        patternLabel: null,
        showEditPatternLabel: false,
        showEditPatternTxtBox: false,
        startAndEndPattern: {
          start: '',
          end: ''
        },
        nDigitPattern: ''
      });
    }
  }
  applyDirective() {
    if (!this.state.pattern) {
      return;
    }
    let directive = `extract-regex-groups ${this.props.column} ${this.state.pattern}`;
    execute([directive])
      .subscribe(
        () => {
          this.props.onComplete();
        },
        (err) => {
          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
  }

  setCustomPattern(e) {
    this.setState({
      pattern: e.target.value
    });
  }

  setStartEndPattern(id, e) {
    let pattern;
    let end;
    let start;
    if (id === 'start') {
      start = e.target.value;
      end = this.state.startAndEndPattern.end;
    }
    if (id === 'end') {
      end = e.target.value;
      start = this.state.startAndEndPattern.start;
    }
    if (start && end) {
      pattern = this.patterns.find(pattern => pattern.patternName === 'startend');
      pattern = pattern ? pattern.getRegex(start, end) : null;
    }
    this.setState({
      pattern,
      startAndEndPattern: {
        start,
        end
      }
    });
  }
  setNDigitPattern(e) {
    let value = parseInt(e.target.value, 10);
    if (isEmpty(e.target.value)) {
      this.setState({
        pattern: null,
        nDigitPattern: null
      });
      return;
    }
    if (typeof value !== 'number' || isNaN(value)) {
      return;
    }
    this.setState({
      nDigitPattern: value,
      pattern: this.patterns.find(pattern => pattern.patternName === 'ndigitnumber').getRegex(value)
    });
  }

  renderStartAndEndPatternContent() {
    return (
      <span>
        {T.translate(`${PREFIX}.startEndPatternContent.description1`)}
        <Input
          className="mousetrap"
          placeholder={`${T.translate(`${PREFIX}.exampleLabel`)} <`}
          onChange={this.setStartEndPattern.bind(this, 'start')}
          value={this.state.startAndEndPattern.start}
          autoFocus
        />
        {T.translate(`${PREFIX}.startEndPatternContent.description2`)}
        <Input
          className="mousetrap"
          placeholder={`${T.translate(`${PREFIX}.exampleLabel`)} >`}
          onChange={this.setStartEndPattern.bind(this, 'end')}
          value={this.state.startAndEndPattern.end}
        />
      </span>
    );
  }
  renderNDigitNumbersPatternContent() {
    return (
      <span>
        {T.translate(`${PREFIX}.ndigitnumberPatternContent.description1`)}
        <Input
          className="mousetrap"
          placeholder={`${T.translate(`${PREFIX}.exampleLabel`)} 3`}
          onChange={this.setNDigitPattern}
          value={this.state.nDigitPattern}
          type="number"
          autoFocus
        />
        {T.translate(`${PREFIX}.ndigitnumberPatternContent.description2`)}
      </span>
    );
  }
  renderCustomPatternContent() {
    return (
      <span className="custom-pattern">
        {T.translate(`${PREFIX}.customPatternContent.description`)}
        <Input
          className="mousetrap"
          placeholder={`${T.translate(`${PREFIX}.exampleLabel`)} [^(]+\(([0-9]{4})\).* `}
          onChange={this.setCustomPattern}
          autoFocus
        />
      </span>
    );
  }
  renderPatternDetails() {
    switch (this.state.patternName) {
      case 'startend':
        return this.renderStartAndEndPatternContent();
      case 'ndigitnumber':
        return this.renderNDigitNumbersPatternContent();
      case 'custom':
        return this.renderCustomPatternContent();
      default:
        return null;
    }
  }

  render() {
    let showHideLink = null;
    if (this.state.showEditPatternLabel) {
      if (this.state.showEditPatternTxtBox) {
        showHideLink = T.translate(`${PREFIX}.hidePatternLabel`);
      } else {
        showHideLink = T.translate(`${PREFIX}.showPatternLabel`);
      }
      showHideLink = (
        <div
          className="showhidelink"
          onClick={this.toggleShowEditPatternTxtBox}
        >
          {showHideLink}
        </div>
      );
    }
    return (
      <Modal
        isOpen={this.props.isOpen}
        toggle={this.props.onClose}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal using-patterns-modal"
      >
        <ModalHeader>

          <span>
            {T.translate(`${PREFIX}.modalTitle`, {parser: 'Log'})}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.onClose}
          >
            <span className="fa fa-times" />
          </div>

        </ModalHeader>
        <ModalBody>

          <p>{T.translate(`${PREFIX}.patternDescription`, {column: this.props.column})}</p>
          {this.patterns[0].patternLabel}
          <UncontrolledDropdown>
            <DropdownToggle caret>
              {
                isNil(this.state.patternLabel) ?
                  this.patterns[0].label
                :
                  this.state.patternLabel
              }
            </DropdownToggle>

            <DropdownMenu>
              <div className="using-patterns-modal-dropdown">
                {
                  this.patterns.map((pattern, index) => {
                    return (
                      <DropdownItem onClick={this.onPatternChange.bind(this, index)}>
                        <span>{pattern.label}</span>
                      </DropdownItem>
                    );
                  })
                }
              </div>
            </DropdownMenu>

          </UncontrolledDropdown>
          {showHideLink}

          <div className="pattern-details">
            {
              !this.state.showEditPatternTxtBox ?
                this.renderPatternDetails()
              :
                null
            }
          </div>

          <div>
            {
              this.state.showEditPatternTxtBox ?
                <Input
                  className="mousetrap"
                  onChange={this.onRawPatternChange}
                  value={this.state.pattern}
                  autoFocus
                />
              :
                null
            }
          </div>
        </ModalBody>
        <ModalFooter>
          <span className="text-warning">
            {this.state.error}
          </span>
          <button
            className="btn btn-primary"
            onClick={this.applyDirective}
            disabled={isNil(this.state.pattern) ? 'disabled' : null}
          >
            {T.translate('features.DataPrep.Directives.ExtractFields.extractBtnLabel')}
          </button>
          <button
            className="btn btn-secondary"
            onClick={this.props.onClose}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
        </ModalFooter>
      </Modal>
    );
  }
}

UsingPatternsModal.propTypes = {
  onClose: PropTypes.func,
  isOpen: PropTypes.isOpen,
  column: PropTypes.string,
  onComplete: PropTypes.func
};
