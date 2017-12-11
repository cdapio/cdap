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
import React, {Component} from 'react';
import IconSVG from 'components/IconSVG';
import ConfirmationModal from 'components/ConfirmationModal';
import {preventPropagation} from 'services/helpers';

export default class DeleteEntityBtn extends Component {
  static propTypes = {
    confirmFn: PropTypes.func.isRequired,
    headerTitle: PropTypes.string,
    confirmationElem: PropTypes.element,
    btnLabel: PropTypes.oneOfType([PropTypes.string, PropTypes.element]),
    className: PropTypes.string
  };
  getDefaulState = () => ({
    showModal: false,
    isLoading: true,
    errorMessage: false,
    extendedMessage: false,
  });
  state = this.getDefaulState();

  toggleModal = (e) => {
    this.setState({
      ...this.getDefaulState(),
      isLoading: !this.state.isLoading,
      showModal: !this.state.showModal
    });
    if (e) {
      preventPropagation(e);
    }
    return false;
  };

  setError = (errorMessage, extendedMessage) => {
    this.setState({
      errorMessage,
      extendedMessage,
      isLoading: false
    });
  }

  confirmFn = () => {
    this.setState({isLoading: true});
    this.props.confirmFn(this.toggleModal, this.setError);
  };
  render() {
    return (
      <span
        className={this.props.className}
        onClick={this.toggleModal}
      >
        {
          this.props.btnLabel ? this.props.btnLabel : <IconSVG name="icon-trash" />
        }
        {
          this.state.showModal ?
            <ConfirmationModal
              isOpen={this.state.showModal}
              cancelFn={this.toggleModal}
              confirmFn={this.confirmFn}
              isLoading={this.state.isLoading}
              toggleModal={this.toggleModal}
              headerTitle={this.props.headerTitle}
              errorMessage={this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
              confirmationElem={this.props.confirmationElem}
            />
          :
            null
        }
      </span>
    );
  }
}
