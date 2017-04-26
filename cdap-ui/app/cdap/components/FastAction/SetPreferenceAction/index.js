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
import FastActionButton from '../FastActionButton';
import T from 'i18n-react';
import {Tooltip} from 'reactstrap';
import classnames from 'classnames';
import SetPreferenceModal from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';
import NamespaceStore from 'services/NamespaceStore';
require('./SetPreferenceAction.scss');

export default class SetPreferenceAction extends Component {
  constructor(props) {
    super(props);

    this.state = {
      modal: false,
      preferencesSaved: false
    };

    this.namespace = NamespaceStore.getState().selectedNamespace;

    this.subscription = NamespaceStore.subscribe(() => {
      this.namespace = NamespaceStore.getState().selectedNamespace;
    });

    this.toggleModal = this.toggleModal.bind(this);
    this.toggleTooltip = this.toggleTooltip.bind(this);
    this.onSuccess = this.onSuccess.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    this.setState({preferencesSaved: nextProps.savedMessageState});
  }

  componentWillUnmount() {
    this.subscription();
  }

  onSuccess() {
    if (this.props.onSuccess) {
      this.props.onSuccess();
    }
    this.setState({preferencesSaved: true});
    setTimeout(() => {
      this.setState({preferencesSaved: false});
    }, 3000);
  }

  toggleTooltip() {
    this.setState({ tooltipOpen : !this.state.tooltipOpen });
  }

  toggleModal() {
    if (this.props.modalIsOpen) {
      this.props.modalIsOpen(!this.state.modal);
    }
    this.setState({modal: !this.state.modal});
  }

  render() {
    const actionLabel = T.translate('features.FastAction.setPreferencesActionLabel');
    let iconClasses = classnames({'fa-lg': this.props.setAtNamespaceLevel}, {'text-success': this.state.preferencesSaved});
    let tooltipID = `${this.namespace}-setpreferences`;
    if (this.props.entity) {
      tooltipID = `${this.props.entity.uniqueId}-setpreferences`;
    }
    return (
      <span className="btn btn-secondary btn-sm">
        <FastActionButton
          icon='icon-wrench'
          iconClasses={iconClasses}
          action={this.toggleModal}
          id={tooltipID}
        />
        <Tooltip
          placement="top"
          isOpen={this.state.tooltipOpen}
          target={tooltipID}
          toggle={this.toggleTooltip}
          className="preferences-action-tooltip"
          delay={0}
        >
          {actionLabel}
        </Tooltip>

        {
          this.state.modal ?
            <SetPreferenceModal
              isOpen={this.state.modal}
              toggleModal={this.toggleModal}
              entity={this.props.entity}
              onSuccess={this.onSuccess}
            />
          :
            null
        }
      </span>
    );
  }
}

SetPreferenceAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    applicationId: PropTypes.string,
    uniqueId: PropTypes.string,
    type: PropTypes.oneOf(['application', 'program']).isRequired,
    programType: PropTypes.string
  }),
  setAtNamespaceLevel: PropTypes.bool,
  modalIsOpen: PropTypes.func,
  onSuccess: PropTypes.func,
  savedMessageState: PropTypes.bool
};

SetPreferenceAction.defaultProps = {
  setAtNamespaceLevel: false,
  modalIsOpen: () => {},
  onSuccess: () => {},
  savedMessageState: false
};
