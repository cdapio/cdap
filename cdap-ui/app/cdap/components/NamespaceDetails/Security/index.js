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
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import T from 'i18n-react';
require('./Security.scss');

const PREFIX = 'features.NamespaceDetails.security';

const mapStateToProps = (state) => {
  return {
    principal: state.principal,
    keytabURI: state.keytabURI
  };
};

const NamespaceDetailsSecurity = ({principal, keytabURI}) => {
  return (
    <div className="namespace-details-security">
      <div className="namespace-details-section-label">
        <strong>{T.translate(`${PREFIX}.label`)}</strong>
      </div>
      <div className="security-values">
        <strong>{T.translate(`${PREFIX}.principal`)}</strong>
        <span>{principal || '- -'}</span>
      </div>
      <div className="security-values">
        <strong>{T.translate(`${PREFIX}.keytabURI`)}</strong>
        <span>{keytabURI || '- -'}</span>
      </div>
    </div>
  );
};

NamespaceDetailsSecurity.propTypes = {
  principal: PropTypes.string,
  keytabURI: PropTypes.string
};

const ConnectedNamespaceDetailsSecurity = connect(mapStateToProps)(NamespaceDetailsSecurity);
export default ConnectedNamespaceDetailsSecurity;
