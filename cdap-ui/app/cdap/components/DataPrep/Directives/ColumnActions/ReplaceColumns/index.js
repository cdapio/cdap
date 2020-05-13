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
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import T from 'i18n-react';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import IconSVG from 'components/IconSVG';
import MouseTrap from 'mousetrap';
import CardActionFeedback, { CARD_ACTION_TYPES } from 'components/CardActionFeedback';
import If from 'components/If';

require('./ReplaceColumns.scss');

const PREFIX = 'features.DataPrep.Directives.ColumnActions.ReplaceColumns';

export default class ReplaceColumns extends Component {
  constructor(props) {
    super(props);

    this.OPTIONS = ['PREFIX', 'PATTERN', 'CUSTOM'];

    this.state = {
      loading: false,
      sourcePattern: '',
      replacePattern: '',
      patternType: this.OPTIONS[0],
      ignoreCase: false,
      error: null,
    };

    this.applyDirective = this.applyDirective.bind(this);
    this.toggleIgnoreCase = this.toggleIgnoreCase.bind(this);
  }

  componentDidMount() {
    MouseTrap.bind('enter', this.applyDirective);
  }

  componentWillUnmount() {
    MouseTrap.unbind('enter');
  }

  handleChange(key, e) {
    this.setState({
      [key]: e.target.value,
    });
  }

  toggleIgnoreCase() {
    this.setState({ ignoreCase: !this.state.ignoreCase });
  }

  selectPatternType(patternType) {
    if (patternType === this.state.patternType) {
      return;
    }

    this.setState({
      patternType,
      sourcePattern: '',
    });
  }

  applyDirective() {
    if (!this.state.sourcePattern) {
      return;
    }

    let replacePattern = this.state.replacePattern;

    let sourcePattern;

    switch (this.state.patternType) {
      case 'PREFIX':
        sourcePattern = `^${this.state.sourcePattern}`;
        break;
      case 'PATTERN':
      case 'CUSTOM':
        sourcePattern = `${this.state.sourcePattern}`;
        break;
    }

    let patternQualifier = 'g';
    if (this.state.ignoreCase) {
      patternQualifier = 'Ig';
    }

    let directive = `columns-replace s/${sourcePattern}/${replacePattern}/${patternQualifier}`;
    this.setState({
      loading: true,
    });
    execute([directive], null, true).subscribe(
      () => {
        this.props.onClose();
      },
      (err) => {
        console.log('error', err);
        this.setState({
          loading: false,
          error: err.message || err.response.message,
        });
      }
    );
  }

  renderPatternTextbox(option) {
    if (option !== this.state.patternType) {
      return null;
    }

    // Have to hardcode this because apparently there's no way to escape
    // [] and {} in i18n-react
    let placeholder = 'e.g. body_[0-9]{2}, eol$';
    if (this.state.patternType === 'PREFIX') {
      placeholder = T.translate(`${PREFIX}.PatternInputPlaceholder.PREFIX`);
    }

    return (
      <div className="clearfix pattern-input">
        {this.state.patternType === 'CUSTOM' ? (
          <label className="replace-label control-label">
            {T.translate(`${PREFIX}.replaceLabel`)}
          </label>
        ) : null}
        <div className="col-12">
          <input
            type="text"
            className="form-control mousetrap"
            value={this.state.sourcePattern}
            onChange={this.handleChange.bind(this, 'sourcePattern')}
            placeholder={placeholder}
            autoFocus
          />
        </div>
      </div>
    );
  }

  renderReplaceWithTextbox() {
    if (this.state.patternType !== 'CUSTOM') {
      return null;
    }

    return (
      <div className="form-group clearfix pattern-input">
        <label className="control-label">{T.translate(`${PREFIX}.withLabel`)}</label>
        <div className="col-12">
          <input
            type="text"
            className="form-control mousetrap"
            value={this.state.replacePattern}
            onChange={this.handleChange.bind(this, 'replacePattern')}
            placeholder={T.translate(`${PREFIX}.replaceWithPlaceholder`)}
          />
        </div>
      </div>
    );
  }

  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="changecolumns-columnactions-modal cdap-modal"
        autoFocus={false}
      >
        <ModalHeader>
          <span>{T.translate(`${PREFIX}.modalTitle`)}</span>

          <div className="close-section float-right" onClick={this.props.onClose}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div>
            {this.OPTIONS.map((option) => {
              return (
                <div key={option}>
                  <div className="option-item" onClick={this.selectPatternType.bind(this, option)}>
                    <span className="fa fa-fw">
                      <IconSVG
                        name={option === this.state.patternType ? 'icon-circle' : 'icon-circle-o'}
                      />
                    </span>

                    <span className="option-label">
                      {T.translate(`${PREFIX}.PatternTypeLabel.${option}`)}
                    </span>
                  </div>

                  {this.renderPatternTextbox(option)}
                </div>
              );
            })}
          </div>

          {this.renderReplaceWithTextbox()}

          <br />

          <div className="ignore-case-line" onClick={this.toggleIgnoreCase}>
            <span className="fa fa-fw">
              <IconSVG name={this.state.ignoreCase ? 'icon-check-square' : 'icon-square-o'} />
            </span>

            <span>{T.translate(`${PREFIX}.ignoreCase`)}</span>
          </div>
        </ModalBody>
        <ModalFooter>
          <fieldset disabled={this.state.loading}>
            <button
              className="btn btn-primary"
              onClick={this.applyDirective}
              disabled={!isNil(this.state.error) || isEmpty(this.state.sourcePattern)}
            >
              {this.state.loading ? <span className="fa fa-spin fa-spinner" /> : null}
              <span className="apply-label">{T.translate(`${PREFIX}.applyButton`)}</span>
            </button>
            <button className="btn btn-secondary" onClick={this.props.onClose}>
              {T.translate('features.DataPrep.Directives.cancel')}
            </button>
          </fieldset>
        </ModalFooter>
        <If condition={this.state.error}>
          <CardActionFeedback type={CARD_ACTION_TYPES.DANGER} message={this.state.error} />
        </If>
      </Modal>
    );
  }
}

ReplaceColumns.propTypes = {
  onClose: PropTypes.func,
};
