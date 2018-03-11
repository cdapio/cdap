/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import NamespaceStore from 'services/NamespaceStore';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import FastActionButton from '../FastActionButton';
import ConfirmationModal from 'components/ConfirmationModal';
import {Tooltip} from 'reactstrap';
import T from 'i18n-react';
import classnames from 'classnames';
import { getType } from 'services/metadata-parser';

export default class TruncateAction extends Component {
  constructor(props) {
    super(props);

    this.action = this.action.bind(this);
    this.toggleModal = this.toggleModal.bind(this);

    this.state = {
      modal: false,
      loading: false,
      errorMessage: '',
      success: false,
      extendedMessage: '',
      tooltipOpen: false
    };
    this.toggleTooltip = this.toggleTooltip.bind(this);
  }

  toggleModal(event) {
    this.setState({modal: !this.state.modal});
    if (event) {
      event.stopPropagation();
      event.nativeEvent.stopImmediatePropagation();
    }
  }
  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }
  onSuccess(res) {
    this.setState({
      success: true
    }, () => {
      if (this.props.onSuccess) {
        this.props.onSuccess(res);
      }
      setTimeout(() => {
        this.setState({
          success: false
        });
      }, 3000);
    });
  }
  action() {
    this.setState({loading: true});
    let api;
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace
    };
    switch (this.props.entity.type) {
      case 'datasetinstance':
        api = MyDatasetApi.truncate;
        params.datasetId = this.props.entity.id;
        break;
      case 'stream':
        api = MyStreamApi.truncate;
        params.streamId = this.props.entity.id;
        break;
    }

    api(params)
      .subscribe((res) => {
        this.onSuccess(res);
        this.setState({
          loading: false,
          modal: false
        });
      }, (err) => {
        this.setState({
          loading: false,
          errorMessage: T.translate('features.FastAction.truncateFailed', {entityId: this.props.entity.id}),
          extendedMessage: err
        });
      });
  }

  render() {
    const actionLabel = T.translate('features.FastAction.truncateLabel');
    const type = getType(this.props.entity);
    const headerTitle = `${actionLabel} ${type}`;
    const tooltipID = `truncate-${this.props.entity.uniqueId}`;
    let truncateActionClassNames = 'icon-cut';
    return (
      <span className="btn btn-secondary btn-sm">
        <FastActionButton
          icon={classnames(truncateActionClassNames, {'text-success': this.state.success})}
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
          {T.translate('features.FastAction.truncateLabel')}
        </Tooltip>

        {
          this.state.modal ? (
            <ConfirmationModal
              headerTitle={headerTitle}
              toggleModal={this.toggleModal}
              confirmationText={T.translate('features.FastAction.truncateConfirmation', {entityId: this.props.entity.id})}
              confirmButtonText={actionLabel}
              confirmFn={this.action}
              cancelFn={this.toggleModal}
              isOpen={this.state.modal}
              isLoading={this.state.loading}
              errorMessage={this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
            />
          ) : null
        }
      </span>
    );
  }
}

TruncateAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onSuccess: PropTypes.func
};
