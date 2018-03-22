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
import IconSVG from 'components/IconSVG';
require('./TopPanel.scss');

const PREFIX = 'features.NamespaceDetails';

const mapStateToProps = (state) => {
  return {
    name: state.name
  };
};

const NamespaceDetailsTopPanel = ({name}) => {
  return (
    <div className="namespace-details-top-panel">
      <span>
        {T.translate(`${PREFIX}.namespaceName`, {namespace: name})}
      </span>
      <IconSVG
        name="icon-close"
        className="close-namespace-details"
        onClick={() => window.history.back()}
      />
    </div>
  );
};

NamespaceDetailsTopPanel.propTypes = {
  name: PropTypes.string
};

const ConnectedNamespaceDetailsTopPanel = connect(mapStateToProps)(NamespaceDetailsTopPanel);
export default ConnectedNamespaceDetailsTopPanel;
