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
import DataPrepStore from 'components/DataPrep/store';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import { preventPropagation } from 'services/helpers';
import CardActionFeedback, { CARD_ACTION_TYPES } from 'components/CardActionFeedback';
import If from 'components/If';

require('./Bulkset.scss');

const PREFIX = 'features.DataPrep.Directives.ColumnActions.Bulkset';
const DATAPREP_PREFIX = 'features.DataPrep.Directives';
const TEXTAREA_ROWS = 10;

export default class Bulkset extends Component {
  constructor(props) {
    super(props);
    this.state = {
      columnNames: '',
      error: null,
      backendError: null,
      loading: false,
    };
    this.applyDirective = this.applyDirective.bind(this);
    this.onColumnNamesChange = this.onColumnNamesChange.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }

  applyDirective() {
    let columns = this.state.columnNames.toString();
    let directive = `set columns ${columns}`;
    this.setState({
      loading: true,
      backendError: null,
    });
    execute([directive], null, true).subscribe(
      () => {
        this.props.onClose();
      },
      (err) => {
        this.setState({
          loading: false,
          backendError:
            err.message ||
            err.response.message ||
            T.translate(`${DATAPREP_PREFIX}.failedApplyDirectiveMessage`),
        });
      }
    );
  }
  validateSpecialChars(value) {
    let regex = /^[\w\d\\\n,]*$/;
    let obj = {
      error: null,
    };
    if (!regex.test(value)) {
      obj = Object.assign({}, obj, {
        error: 'No special characters allowed',
      });
    }
    return obj;
  }
  validateColumnCount(value) {
    let obj = {
      error: null,
    };
    let headers = DataPrepStore.getState().dataprep.headers;
    let columnNames = value.split(',');
    if (headers.length !== columnNames.length) {
      obj = Object.assign({}, obj, {
        error: 'Number of columns has to be the same',
      });
    }
    return obj;
  }
  validateInput(value) {
    let obj = {};
    obj = Object.assign({}, obj, this.validateSpecialChars(value));
    if (!obj.error) {
      obj = Object.assign({}, obj, this.validateColumnCount(value));
    }
    return obj;
  }
  onColumnNamesChange(e) {
    let value = e.target.value;
    value = value.trim();
    let obj = {
      columnNames: value,
    };
    obj = Object.assign({}, obj, this.validateInput(value));
    this.setState(obj);
  }
  handleKeyPress(e) {
    if (
      e.nativeEvent.keyCode !== 13 ||
      !isNil(this.state.error) ||
      isEmpty(this.state.columnNames)
    ) {
      return;
    }

    this.applyDirective();
    preventPropagation(e);
  }
  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="bulkset-columnactions-modal cdap-modal"
        autoFocus={false}
      >
        <ModalHeader>
          <span>{T.translate(`${PREFIX}.modalTitle`)}</span>

          <div className="close-section float-right" onClick={this.props.onClose}>
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <fieldset disabled={this.state.loading}>
            <p>{T.translate(`${PREFIX}.description`)}</p>
            <textarea
              placeholder={T.translate(`${PREFIX}.textareaplaceholder`)}
              rows={TEXTAREA_ROWS}
              className="form-control mousetrap"
              value={this.state.columnNames}
              onChange={this.onColumnNamesChange}
              onKeyPress={this.handleKeyPress}
              autoFocus
            />
            <div className="text-danger">{this.state.error}</div>
          </fieldset>
        </ModalBody>
        <ModalFooter>
          <fieldset disabled={this.state.loading}>
            <button
              className="btn btn-primary"
              onClick={this.applyDirective}
              disabled={!isNil(this.state.error) || isEmpty(this.state.columnNames)}
            >
              {this.state.loading ? <span className="fa fa-spin fa-spinner" /> : null}
              <span className="apply-label">
                {T.translate('features.DataPrep.Directives.apply')}
              </span>
            </button>
            <button className="btn btn-secondary" onClick={this.props.onClose}>
              {T.translate('features.DataPrep.Directives.cancel')}
            </button>
          </fieldset>
        </ModalFooter>
        <If condition={this.state.backendError}>
          <CardActionFeedback type={CARD_ACTION_TYPES.DANGER} message={this.state.backendError} />
        </If>
      </Modal>
    );
  }
}
Bulkset.propTypes = {
  onClose: PropTypes.func.isRequired,
};
