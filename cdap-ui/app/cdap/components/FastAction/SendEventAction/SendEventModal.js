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
import {Modal, ModalHeader, ModalBody} from 'reactstrap';
import FileDataUpload from 'components/FileDataUpload';
import classnames from 'classnames';
import {MyStreamApi} from 'api/stream';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
import {Observable} from 'rxjs/Observable';
import cookie from 'react-cookie';
import NamespaceStore from 'services/NamespaceStore';
import isEmpty from 'lodash/isEmpty';

import T from 'i18n-react';

export default class SendEventModal extends Component {
  constructor(props) {
    super(props);
    this.state = this.getDefaultState();
    this.sendEvents = this.sendEvents.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onDrop = this.onDrop.bind(this);
    this.noInputYet = this.noInputYet.bind(this);
    this.clearEvents = this.clearEvents.bind(this);
  }

  getDefaultState() {
    return {
      loading: false,
      statusMessage: '',
      extendedMessage: '',
      textInput: '',
      droppedFile: {},
      tooltipOpen: false,
      responseStatus: '',
      reset: 0
    };
  }

  clearEvents() {
    let finalState = Object.assign({}, this.getDefaultState(), {
      modal: true,
      reset: ++this.state.reset
    });
    this.setState(finalState);
  }

  sendEvents(e) {
    e.stopPropagation();
    e.preventDefault();
    e.nativeEvent.stopImmediatePropagation();
    this.setState({loading: true});
    let namespace = NamespaceStore.getState().selectedNamespace;
    let streamId = this.props.entity.id;
    let params = {
      namespace
    };
    let subscriptions = [];
    if (isEmpty(this.state.droppedFile)) {
      let events = this.state.textInput.replace(/\r\n/g, '\n').split('\n');
      params.streamId = streamId;
      events.forEach(event => {
        subscriptions.push(
          MyStreamApi.sendEvent(params, event)
        );
      });
      this.setState({
        loading: true
      });
      let mergedOb = Observable.merge.apply(null, subscriptions);
      mergedOb.subscribe(
        () => {
          this.setState(Object.assign({}, this.getDefaultState(), {
            modal: true,
            responseStatus: 200,
            reset: ++this.state.reset,
            statusMessage: T.translate('features.FastAction.sendEventsSuccess')
          }));
        },
        (err) => {
          this.setState(Object.assign({}, this.getDefaultState(), {
            modal: true,
            responseStatus: 500,
            reset: ++this.state.reset,
            statusMessage: T.translate('features.FastAction.sendEventsFailed'),
            extendedMessage: err,
          }));
        }
      );
      return false;
    }

    let url = `/namespaces/${namespace}/streams/${streamId}/batch`;
    let fileContents = this.state.droppedFile;
    let filename = this.state.droppedFile.name;
    let filetype = 'text/' + filename.split('.').pop();
    let authToken = cookie.load('CDAP_Auth_Token');
    this.setState({
      loading: true
    });
    return UploadDataActionCreator
      .uploadData({
        url,
        fileContents,
        headers: {
          filename,
          filetype,
          authToken
        }
      })
      .subscribe(() => {
        this.setState(Object.assign({}, this.getDefaultState(), {
          modal: true,
          reset: ++this.state.reset,
          responseStatus: 200,
          statusMessage: T.translate('features.FastAction.sendEventsSuccess')
        }));
      }, (err) => {
        this.setState(Object.assign({}, this.getDefaultState(), {
          modal: true,
          responseStatus: 500,
          statusMessage: T.translate('features.FastAction.sendEventsFailed'),
          extendedMessage: err
        }));
      });
  }

  onDrop(file) {
    this.setState({droppedFile : file});
  }

  noInputYet() {
    return isEmpty(this.state.droppedFile) && this.state.textInput === '';
  }

  handleTextInput(input) {
    this.setState({
      textInput : input
    });
  }

  render() {
    const actionLabel = T.translate('features.FastAction.sendEventsLabel');
    const headerTitle = `${actionLabel} to ${this.props.entity.id}`;
    return (
      <Modal
        isOpen={true}
        toggle={this.props.onClose}
        className="confirmation-modal stream-send-events"
        size="lg"
        backdrop='static'
      >
        <ModalHeader>
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
          <div className="events-drop-container">
            <FileDataUpload
              onDataUpload={this.onDrop}
              onTextInput={this.handleTextInput}
              reset={this.state.reset}
            />
            <div className="send-event-button">
              <span>
                {
                  this.state.responseStatus ?
                    <span className={classnames({
                        'text-danger': this.state.responseStatus !== 200,
                        'text-success': this.state.responseStatus === 200
                      })}
                    >
                    {this.state.statusMessage}
                    </span>
                  :
                    null
                }
              </span>

              <div>
                <button
                  className="btn btn-default"
                  onClick={this.clearEvents}
                  disabled={(this.noInputYet() || this.state.loading) ? 'disabled' : null}
                >
                  <span>{T.translate('features.FastAction.clearEventsButtonLabel')}</span>
                </button>
                <button
                  className="btn btn-primary"
                  onClick={this.sendEvents}
                  disabled={(this.noInputYet() || this.state.loading) ? 'disabled' : null}
                >
                  {
                    this.state.loading ?
                      <span className="fa fa-spinner fa-spin"></span>
                    :
                      null
                  }
                  <span>{T.translate('features.FastAction.sendEventsButtonLabel')}</span>
                </button>
              </div>
            </div>
          </div>
        </ModalBody>
      </Modal>
    );
  }
}

SendEventModal.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onClose: PropTypes.func
};
