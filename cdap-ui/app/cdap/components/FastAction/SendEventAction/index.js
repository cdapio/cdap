/*
 * Copyright © 2016 Cask Data, Inc.
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
import NamespaceStore from 'services/NamespaceStore';
import FastActionButton from '../FastActionButton';
import T from 'i18n-react';
import { Modal, Tooltip, ModalHeader, ModalBody } from 'reactstrap';
import isEmpty from 'lodash/isEmpty';
import {MyStreamApi} from 'api/stream';
import FileDataUpload from 'components/FileDataUpload';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
require('./SendEventAction.less');

import cookie from 'react-cookie';
import Rx from 'rx';

export default class SendEventAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      modal: false,
      loading: false,
      errorMessage: '',
      extendedMessage: '',
      textarea: false,
      textInput: '',
      droppedFile: {},
      tooltipOpen: false,
      error: ''
    };

    this.eventText = '';
    this.action = this.action.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onDrop = this.onDrop.bind(this);
    this.toggleTextareaOff = this.toggleTextareaOff.bind(this);
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.noInputYet = this.noInputYet.bind(this);
  }

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }

  toggleModal(event) {
    this.setState({
      modal: !this.state.modal,
      loading: false,
      errorMessage: '',
      extendedMessage: '',
      textarea: false,
      textInput: '',
      droppedFile: {}
    });
    if (event) {
      event.stopPropagation();
      event.nativeEvent.stopImmediatePropagation();
    }
  }

  action(e) {
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
      let mergedOb = Rx.Observable.merge.apply(null, subscriptions);
      mergedOb.subscribe(this.props.onSuccess.bind(this));
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
      .subscribe(this.props.onSuccess, (err) => {
        this.setState({
          loading: false,
          errorMessage: T.translate('features.FastAction.sendEventsFailed', {entityId: this.props.entity.id}),
          extendedMessage: err
        });
      });
  }

  handleTextInput(input) {
    this.setState({
      textInput : input
    });
  }

  toggleTextareaOff(e) {
    if (this.state.loading) { return; }
    e.preventDefault();
    this.setState({
      textarea : false
    });
  }

  onDrop(file) {
    this.setState({droppedFile : file});
  }

  noInputYet() {
    return isEmpty(this.state.droppedFile) && this.state.textInput === '';
  }

  render() {
    const actionLabel = T.translate('features.FastAction.sendEventsLabel');
    const headerTitle = `${actionLabel} to ${this.props.entity.id}`;
    let tooltipID = `${this.props.entity.uniqueId}-sendevents`;
    return (
      <span>
        <FastActionButton
          icon="fa fa-upload"
          action={this.toggleModal}
          id={tooltipID}
        />
        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          target={tooltipID}
          toggle={this.toggleTooltip}
          delay={0}
        >
          {T.translate('features.FastAction.sendEventsLabel')}
        </Tooltip>

        {
          this.state.modal ? (
            <Modal
              isOpen={this.state.modal}
              toggle={this.toggleModal}
              className="confirmation-modal stream-send-events"
              size="lg"
            >
              <ModalHeader>
                <div className="pull-left">
                  {headerTitle}
                </div>
                <div className="pull-right">
                  <div className="close-modal-btn"
                    onClick={this.toggleModal.bind(this)}
                  >
                    <span
                      className={"button-icon fa fa-times"}
                    />
                  </div>
                </div>
              </ModalHeader>
              <ModalBody
                className="modal-body"
                onClick={this.toggleTextareaOff}
              >
                <div className="events-drop-container">
                  <FileDataUpload onDataUpload={this.onDrop} onTextInput={this.handleTextInput}/>
                  <div className="clearfix send-event-button">
                    {
                      this.state.error ?
                        <span className="pull-left text-danger"></span>
                      :
                        null
                    }
                    <button
                      className="btn btn-primary pull-right"
                      onClick={this.action}
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
              </ModalBody>
            </Modal>
          ) : null
        }
      </span>
    );
  }
}

SendEventAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onSuccess: PropTypes.func
};
