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
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import {MyStreamApi} from 'api/stream';
import FileDnD from 'components/FileDnD';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
require('./SendEventAction.less');
import cookie from 'react-cookie';
import Rx from 'rx';
import classnames from 'classnames';

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
      error: ''
    };

    this.eventText = '';
    this.action = this.action.bind(this);
    this.toggleModal = this.toggleModal.bind(this);
    this.renderStreamEventCopyPaste = this.renderStreamEventCopyPaste.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onDrop = this.onDrop.bind(this);
    this.toggleTextareaOff = this.toggleTextareaOff.bind(this);
    this.FileDnDClickHandler = this.FileDnDClickHandler.bind(this);
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
    if (!Object.keys(this.state.droppedFile).length) {
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

  handleTextInput(e){
    this.setState({
      textInput : e.target.value
    });
  }

  toggleTextareaOff(e){
    if(this.state.loading) { return; }
    e.preventDefault();
    this.setState({
      textarea : false
    });
  }

  onDrop(e) {
    this.setState({droppedFile : e[0]});
  }

  FileDnDClickHandler(e) {
    e.stopPropagation();
    e.preventDefault();
    this.setState({
      textarea : true,
      droppedFile: {}
    });
  }

  renderStreamEventCopyPaste(){
    if(!this.state.textarea){
      return (
        <div
          className={classnames("dropzone-container", {
            "file-dropped": Object.keys(this.state.droppedFile).length
          })}
          onClick={this.FileDnDClickHandler}
        >
          <span className="fa fa-plus-circle fa-3x"></span>
          <FileDnD
            file={this.state.droppedFile}
            clickLabel={T.translate('features.FastAction.sendEventsClickLabel')}
            onDropHandler={this.onDrop}
          />
        </div>
      );
    }

    return (
      <textarea
        autoFocus
        className="form-control"
        onClick={(e) => {
          e.preventDefault();
          e.stopPropagation();
        }}
        onChange={this.handleTextInput}
      />
    );
  }

  render() {
    const actionLabel = T.translate('features.FastAction.sendEventsLabel');
    const headerTitle = `${actionLabel} to ${this.props.entity.id}`;

    return (
      <span>
        <FastActionButton
          icon="fa fa-upload"
          action={this.toggleModal}
        />
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
                  {this.renderStreamEventCopyPaste()}
                  <div className="clearfix">
                    {
                      this.state.error ?
                        <span className="pull-left text-danger"></span>
                      :
                        null
                    }
                    <button
                      className="btn btn-primary pull-right"
                      onClick={this.action}
                      disabled={this.state.loading ? 'disabled' : null}
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
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onSuccess: PropTypes.func
};
