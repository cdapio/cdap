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
import {Modal, ModalHeader, ModalBody, ModalFooter} from 'reactstrap';
import T from 'i18n-react';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import {preventPropagation} from 'services/helpers';
require('./Bulkset.scss');

const PREFIX = 'features.DataPrep.Directives.ColumnActions.Bulkset';
const TEXTAREA_ROWS = 10;

export default class Bulkset extends Component {
  constructor(props) {
    super(props);
    this.state = {
      columnNames: '',
      error: null,
      loading: false
    };
    this.applyDirective = this.applyDirective.bind(this);
    this.onColumnNamesChange = this.onColumnNamesChange.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }
  componentDidMount() {
    if (this.textarea) {
      this.textarea.focus();
    }
  }
  applyDirective() {
    let columns = this.state.columnNames.toString();
    let directive = `set columns ${columns}`;
    this.setState({
      loading: true
    });
    execute([directive], null, true)
      .subscribe(
        () => {
          this.props.onClose();
        },
        (err) => {
          this.setState({
            loading: false,
            error: err.message || err.response.message
          });
        }
      );
  }
  validateSpecialChars(value) {
    let regex = /^[\w\d\\\n,]*$/;
    let obj = {
      error: null
    };
    if (!regex.test(value)) {
      obj = Object.assign({}, obj, {
        error: 'No special characters allowed'
      });
    }
    return obj;
  }
  validateColumnCount(value) {
    let obj = {
      error: null
    };
    let headers = DataPrepStore.getState().dataprep.headers;
    let columnNames = value.split(',');
    if (headers.length !== columnNames.length) {
      obj = Object.assign({}, obj, {
        error: 'Number of columns has to be the same'
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
      columnNames: value
    };
    obj = Object.assign({}, obj, this.validateInput(value));
    this.setState(obj);
  }
  handleKeyPress(e) {
    if (e.nativeEvent.keyCode !== 13 || !isNil(this.state.error) || isEmpty(this.state.columnNames)) { return; }

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
        className="dataprep-parse-modal bulkset-columnactions-modal"
      >
        <ModalHeader>

          <span>
            {T.translate(`${PREFIX}.modalTitle`)}
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.onClose}
          >
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
              ref={ref => this.textarea = ref}
            >
            </textarea>
            <div className="text-danger">
              {this.state.error}
            </div>
          </fieldset>
        </ModalBody>
        <ModalFooter>
          <fieldset disabled={this.state.loading}>
            <button
              className="btn btn-primary"
              onClick={this.applyDirective}
              disabled={!isNil(this.state.error) || isEmpty(this.state.columnNames)}
            >
              {
                this.state.loading ?
                  <span className="fa fa-spin fa-spinner"></span>
                :
                  null
              }
              <span className="apply-label">{T.translate('features.DataPrep.Directives.apply')}</span>
            </button>
            <button
              className="btn btn-secondary"
              onClick={this.props.onClose}
            >
              {T.translate('features.DataPrep.Directives.cancel')}
            </button>
          </fieldset>
        </ModalFooter>
      </Modal>
    );
  }
}
Bulkset.propTypes = {
  onClose: PropTypes.func.isRequired
};
