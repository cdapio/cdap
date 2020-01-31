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

import React from 'react';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
require('./Description.scss');

const mapStateToProps = (state) => {
  return {
    description: state.description,
  };
};

const NamespaceDetailsDescription = ({ description }) => {
  return <div className="namespace-details-description">{description}</div>;
};

NamespaceDetailsDescription.propTypes = {
  description: PropTypes.string,
};

const ConnectedNamespaceDetailsDescription = connect(mapStateToProps)(NamespaceDetailsDescription);
export default ConnectedNamespaceDetailsDescription;
