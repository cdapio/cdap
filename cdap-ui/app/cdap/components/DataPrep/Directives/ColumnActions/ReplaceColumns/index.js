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
import {Modal, ModalHeader, ModalBody, ModalFooter} from 'reactstrap';
import T from 'i18n-react';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';

const PREFIX = 'features.DataPrep.Directives.ColumnActions.ReplaceColumns';

export default class ReplaceColumns extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false,
      sourcePattern: '',
      destination: '',
      error: null
    };

    this.applyDirective = this.applyDirective.bind(this);
  }

  handleChange(key, e) {
    this.setState({
      [key]: e.target.value
    });
  }

  applyDirective() {
    let sourcePattern = this.state.sourcePattern;
    let destination = this.state.destination;
    let directive = `columns-replace s/${sourcePattern}/${destination}/g`;
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

  render() {
    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        size="md"
        backdrop="static"
        zIndex="1061"
        className="dataprep-parse-modal changecolumns-columnactions-modal"
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
          <div className="form-group clearfix">
            <label className="control-label">
              {T.translate(`${PREFIX}.patternLabel`)}
            </label>
            <div className="col-xs-12">
              <input
                type="text"
                className="form-control"
                value={this.state.sourcePattern}
                onChange={this.handleChange.bind(this, 'sourcePattern')}
              />
            </div>
          </div>

          <div className="form-group clearfix">
            <label className="control-label">
              {T.translate(`${PREFIX}.replaceWithLabel`)}
            </label>
            <div className="col-xs-12">
              <input
                type="text"
                className="form-control"
                value={this.state.destination}
                onChange={this.handleChange.bind(this, 'destination')}
              />
            </div>
          </div>
        </ModalBody>
        <ModalFooter>
          <fieldset disabled={this.state.loading}>
            <button
              className="btn btn-primary"
              onClick={this.applyDirective}
              disabled={!isNil(this.state.error) || isEmpty(this.state.sourcePattern)}
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

ReplaceColumns.propTypes = {
  onClose: PropTypes.func
};
