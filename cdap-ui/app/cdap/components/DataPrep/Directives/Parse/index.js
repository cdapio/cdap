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
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import SingleFieldModal from 'components/DataPrep/Directives/Parse/Modals/SingleFieldModal';
import CSVModal from 'components/DataPrep/Directives/Parse/Modals/CSVModal';
import LogModal from 'components/DataPrep/Directives/Parse/Modals/LogModal';
import SimpleDateModal from 'components/DataPrep/Directives/Parse/Modals/SimpleDateModal';
import ExcelModal from 'components/DataPrep/Directives/Parse/Modals/ExcelModal';
import T from 'i18n-react';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import debounce from 'lodash/debounce';
import {setPopoverOffset} from 'components/DataPrep/helper';

const SUFFIX = 'features.DataPrep.Directives.Parse';

require('./ParseDirective.scss');

const DIRECTIVE_MAP = {
  'CSV': 'parse-as-csv',
  'XML': 'parse-as-xml',
  'JSON': 'parse-as-json',
  'XMLTOJSON': 'parse-xml-to-json',
  'LOG': 'parse-as-log',
  'SIMPLEDATE': 'parse-as-simple-date',
  'NATURALDATE': 'parse-as-date',
  'FIXEDLENGTH': 'parse-as-fixed-length',
  'HL7': 'parse-as-hl7',
  'AVRO': 'parse-as-avro-file',
  'EXCEL': 'parse-as-excel'
};

export default class ParseDirective extends Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedParse: null
    };

    this.preventPropagation = this.preventPropagation.bind(this);

    this.PARSE_OPTIONS = [
      'CSV',
      'AVRO',
      'EXCEL',
      'XML',
      'JSON',
      'XMLTOJSON',
      'LOG',
      'SIMPLEDATE',
      'NATURALDATE',
      'FIXEDLENGTH',
      'HL7'
    ];

    window.addEventListener('resize', this.offsetCalcDebounce);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('parse-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  applyDirective(parseOption, configuration) {
    let column = this.props.column;
    let condition = DIRECTIVE_MAP[parseOption];

    let directive = `${condition} :${column}`;

    if (configuration) {
      directive = `${directive} ${configuration}`;
    }

    this.execute([directive]);
  }

  execute(addDirective) {
    execute(addDirective)
      .subscribe(() => {
        this.props.close();
        this.props.onComplete();
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

  selectParse(option) {
    if (['XML', 'HL7', 'AVRO'].indexOf(option) !== -1) {
      this.applyDirective(option);
      return;
    }

    this.setState({selectedParse: option});
  }

  renderSingleFieldModal() {
    let isRequired = this.state.selectedParse === 'FIXEDLENGTH';

    let defaultValue;
    if (['JSON', 'XMLTOJSON'].indexOf(this.state.selectedParse) !== -1) {
      defaultValue = '1';
    }

    let hasOptionalField = this.state.selectedParse === 'FIXEDLENGTH';

    return (
      <SingleFieldModal
        toggle={this.selectParse.bind(this, null)}
        parser={this.state.selectedParse}
        onApply={this.applyDirective.bind(this)}
        isTextRequired={isRequired}
        defaultValue={defaultValue}
        hasOptionalField={hasOptionalField}
      />
    );
  }

  renderCSVModal() {
    return (
      <CSVModal
        toggle={this.selectParse.bind(this, null)}
        onApply={this.applyDirective.bind(this)}
      />
    );
  }

  renderLogModal() {
    return (
      <LogModal
        toggle={this.selectParse.bind(this, null)}
        onApply={this.applyDirective.bind(this)}
      />
    );
  }

  renderSimpleDateModal() {
    return (
      <SimpleDateModal
        source="parse"
        toggle={this.selectParse.bind(this, null)}
        onApply={this.applyDirective.bind(this)}
      />
    );
  }

  renderExcelModal() {
    return (
      <ExcelModal
        toggle={this.selectParse.bind(this, null)}
        onApply={this.applyDirective.bind(this, 'EXCEL')}
      />
    );
  }

  renderModal() {
    if (!this.state.selectedParse) { return null; }

    if (this.state.selectedParse === 'CSV') {
      return this.renderCSVModal();
    } else if (this.state.selectedParse === 'LOG') {
      return this.renderLogModal();
    } else if (this.state.selectedParse === 'SIMPLEDATE') {
      return this.renderSimpleDateModal();
    } else if (this.state.selectedParse === 'EXCEL') {
      return this.renderExcelModal();
    } else {
      return this.renderSingleFieldModal();
    }
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="parse-detail second-level-popover"
        onClick={this.preventPropagation}
      >
        <div className="parse-options">
          {
            this.PARSE_OPTIONS.map((option) => {
              return (
                <div
                  key={option}
                  className="option"
                  onClick={this.selectParse.bind(this, option)}
                >
                  {T.translate(`${SUFFIX}.Parsers.${option}.label`)}
                </div>
              );
            })
          }
        </div>

        {this.renderModal()}
      </div>
    );
  }

  render() {
    return (
      <div
        id="parse-directive"
        className={classnames('parse-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>
          {T.translate(`${SUFFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
      </div>
    );
  }
}

ParseDirective.propTypes = {
  column: PropTypes.string,
  onComplete: PropTypes.func,
  isOpen: PropTypes.bool,
  close: PropTypes.func
};
