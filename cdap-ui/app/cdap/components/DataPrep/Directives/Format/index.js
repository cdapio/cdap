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
import classnames from 'classnames';
import T from 'i18n-react';
import SimpleDateModal from 'components/DataPrep/Directives/Parse/Modals/SimpleDateModal';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';

const PREFIX = 'features.DataPrep.Directives.Format';

export default class Format extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeModal: null
    };
    this.applyDirective = this.applyDirective.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
    this.renderModal = this.renderModal.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);

    this.applyDateFormat = this.applyDateFormat.bind(this);
    this.formatToDateTime = this.formatToDateTime.bind(this);
    this.formatToUppercase = this.formatToUppercase.bind(this);
    this.formatToLowercase = this.formatToLowercase.bind(this);
    this.formatTitlecase = this.formatTitlecase.bind(this);

    this.FORMAT_OPTIONS = [
      {
        name: 'TO_DATE_TIME',
        onClick: this.formatToDateTime
      },
      {
        name: 'TO_UPPERCASE',
        onClick: this.formatToUppercase
      },
      {
        name: 'TO_LOWERCASE',
        onClick: this.formatToLowercase
      },
      {
        name: 'TO_TITLECASE',
        onClick: this.formatTitlecase
      }
    ];
  }
  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('format-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }
  toggleModal() {
    this.setState({
      activeModal: null
    });
  }
  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }
  applyDirective(directive) {
    execute([directive])
      .subscribe(
        this.toggleModal,
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
  applyDateFormat(name, format) {
    let directive = `format-date ${this.props.column} ${format}`;
    this.applyDirective(directive);
  }
  formatToUppercase() {
    this.applyDirective(`uppercase ${this.props.column}`);
  }
  formatToLowercase() {
    this.applyDirective(`lowercase ${this.props.column}`);
  }
  formatTitlecase() {
    this.applyDirective(`titlecase ${this.props.column}`);
  }
  formatToDateTime() {
    this.setState({
      activeModal: (
        <SimpleDateModal
          source="format"
          toggle={this.toggleModal}
          onApply={this.applyDateFormat}
        />
      )
    });
  }
  renderModal() {
    return this.state.activeModal;
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
            this.FORMAT_OPTIONS.map((option) => {
              return (
                <div
                  key={option.name}
                  className="option"
                  onClick={option.onClick}
                >
                  {T.translate(`${PREFIX}.Formats.${option.name}.label`)}
                </div>
              );
            })
          }
        </div>
      </div>
    );
  }
  render() {
    return (
      <div
        id="format-directive"
        className={classnames('parse-directive clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span>
          {T.translate(`${PREFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
        {this.renderModal()}
      </div>
    );
  }
}
Format.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.string
};
