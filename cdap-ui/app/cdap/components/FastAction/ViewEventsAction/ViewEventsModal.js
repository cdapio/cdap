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
import Datetime from 'react-datetime';
import {MyStreamApi} from 'api/stream';
import NamespaceStore from 'services/NamespaceStore';
import shortid from 'shortid';
import CardActionFeedback from 'components/CardActionFeedback';

require('./ViewEventsModal.scss');

/**
 * Datetime component will parse date as moment.js object
 **/
export default class ViewEventsModal extends Component {
  constructor(props) {
    super(props);

    const NOW = Date.now();
    const ONE_HOUR_AGO = NOW - (60 * 60 * 1000);

    this.state = {
      fromDate: ONE_HOUR_AGO,
      toDate: NOW,
      limit: 10,
      events: [],
      error: null
    };

    this.changeFromDate = this.changeFromDate.bind(this);
    this.changeToDate = this.changeToDate.bind(this);
    this.handleLimitChange = this.handleLimitChange.bind(this);
    this.viewEvents = this.viewEvents.bind(this);

    this.viewEvents();
  }

  changeFromDate(date) {
    this.setState({fromDate: date.valueOf()});
  }
  changeToDate(date) {
    this.setState({toDate: date.valueOf()});
  }
  handleLimitChange(e) {
    this.setState({limit: e.target.value});
  }

  renderFromDatetime() {
    return (
      <div className="input-container">
        <Datetime
          value={this.state.fromDate}
          onChange={this.changeFromDate}
        />
      </div>
    );
  }

  renderToDatetime() {
    return (
      <div className="input-container">
        <Datetime
          value={this.state.toDate}
          onChange={this.changeToDate}
        />
      </div>
    );
  }

  viewEvents() {
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace,
      streamId: this.props.entity.id,
      start: this.state.fromDate,
      end: this.state.toDate
    };

    if (this.state.limit) {
      params.limit = this.state.limit;
    }

    MyStreamApi.viewEvents(params)
      .subscribe((res = []) => {
        let events = res.map((row) => {
          row.uniqueId = shortid.generate();
          row.headers = JSON.stringify(row.headers);

          return row;
        });

        this.setState({
          events,
          error: null
        });
      }, (error) => {
        this.setState({
          error,
          events: []
        });
      });
  }

  renderResults() {
    if (this.state.events.length === 0) {
      return (
        <div>
          {T.translate('features.FastAction.viewEvents.noResults')}
        </div>
      );
    }

    const headers = ['timestamp', 'headers', 'body'];

    return (
      <div className="results">
        <table className="table table-bordered">
          <thead>
            {
              headers.map((head) => {
                return <th key={head}>{head}</th>;
              })
            }
          </thead>

          <tbody>
            {
              this.state.events.map((row) => {
                return (
                  <tr key={row.uniqueId}>
                    {
                      headers.map((head) => {
                        return <td key={head}>{row[head]}</td>;
                      })
                    }
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }

  renderFooter() {
    if (!this.state.error) { return null; }

    return (
      <ModalFooter>
        <CardActionFeedback
          type='DANGER'
          message={T.translate('features.FastAction.viewEvents.failedMessage')}
          extendedMessage={this.state.error}
        />
      </ModalFooter>
    );
  }

  render() {
    const headerTitle = T.translate('features.FastAction.viewEvents.label');

    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        className="confirmation-modal view-events-modal"
        size="lg"
        backdrop='static'
      >
        <ModalHeader className="clearfix">
          <div className="float-xs-left">
            {headerTitle}
          </div>
          <div className="float-xs-right">
            <div
              className="close-modal-btn"
              onClick={this.props.onClose}
            >
              <span className={"button-icon fa fa-times"}></span>
            </div>
          </div>
        </ModalHeader>
        <ModalBody className="modal-body">
          <h4>
            {T.translate('features.FastAction.viewEvents.modalHeader', {
              entityId: this.props.entity.id
            })}
          </h4>

          <div className="row">
            <div className="col-xs-8">
              <h6>{T.translate('features.FastAction.viewEvents.timeRangeTitle')}</h6>

              <span className="date-container">
                <strong>
                  {T.translate('features.FastAction.viewEvents.from')}
                </strong>
                {this.renderFromDatetime()}
              </span>

              <span className="date-container">
                <strong>
                  {T.translate('features.FastAction.viewEvents.to')}
                </strong>
                {this.renderToDatetime()}
              </span>
            </div>

            <div className="col-xs-4">
              <h6>{T.translate('features.FastAction.viewEvents.numEventsTitle')}</h6>

              <strong>
                {T.translate('features.FastAction.viewEvents.limit')}
              </strong>
              <div className="input-container">
                <input
                  type="number"
                  min={1}
                  className="form-control"
                  value={this.state.limit}
                  onChange={this.handleLimitChange}
                />
              </div>
            </div>
          </div>

          <div className="button-container">
            <button
              className="btn btn-primary"
              onClick={this.viewEvents}
            >
              {T.translate('features.FastAction.viewEvents.button')}
            </button>
          </div>

          <hr/>

          {this.renderResults()}
        </ModalBody>

        {this.renderFooter()}
      </Modal>
    );
  }
}

ViewEventsModal.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['stream']).isRequired,
  }),
  onClose: PropTypes.func.isRequired
};
