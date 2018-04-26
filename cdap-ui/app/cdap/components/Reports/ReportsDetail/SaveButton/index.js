/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import SaveModal from 'components/Reports/ReportsDetail/SaveButton/SaveModal';

require('./SaveButton.scss');

class SaveButtonView extends Component {
  static propTypes = {
    expiry: PropTypes.number,
    name: PropTypes.string,
    reportId: PropTypes.string
  };

  state = {
    showModal: false
  };

  toggleModal = () => {
    this.setState({
      showModal: !this.state.showModal
    });
  };

  renderModal = () => {
    if (!this.state.showModal) { return null; }

    return (
      <SaveModal
        name={this.props.name}
        toggle={this.toggleModal}
        reportId={this.props.reportId}
      />
    );
  }

  render() {
    if (!this.props.expiry) { return null; }

    return (
      <span>
        <button
          className="btn btn-primary"
          onClick={this.toggleModal}
        >
          Save Report
        </button>

        {this.renderModal()}
      </span>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    expiry: state.details.expiry,
    name: state.details.name,
    reportId: state.details.reportId
  };
};

const SaveButton = connect(
  mapStateToProps
)(SaveButtonView);

export default SaveButton;
