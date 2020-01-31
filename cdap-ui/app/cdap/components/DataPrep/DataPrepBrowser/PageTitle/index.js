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

import PropTypes from 'prop-types';
import React from 'react';
import Helmet from 'react-helmet';
import { connect } from 'react-redux';
import T from 'i18n-react';
import { isNilOrEmpty, objectQuery } from 'services/helpers';
import { Theme } from 'services/ThemeHelper';
const PREFIX = 'features.DataPrep.DataPrepBrowser';

function DataPrepBrowserPageTitle({ connectionId, browserI18NName, path }) {
  let title = T.translate(`${PREFIX}.${browserI18NName}.pageTitle`, {
    connectionId,
    productName: Theme.productName,
  });
  if (!isNilOrEmpty(path)) {
    title = `${title} - ${path}`;
  }
  return <Helmet title={title} />;
}
DataPrepBrowserPageTitle.propTypes = {
  connectionId: PropTypes.string,
  browserI18NName: PropTypes.string,
  path: PropTypes.string,
};
const mapStateToProps = (state, ownProps) => ({
  connectionId: state[ownProps.browserStateName].connectionId,
  path:
    isNilOrEmpty(ownProps.locationToPathInState) || !Array.isArray(ownProps.locationToPathInState)
      ? null
      : objectQuery(state, ownProps.browserStateName, ...ownProps.locationToPathInState),
});

const ConnectedDataPrepBrowserPageTitle = connect(mapStateToProps)(DataPrepBrowserPageTitle);
export default ConnectedDataPrepBrowserPageTitle;
