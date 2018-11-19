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
import * as React from 'react';
import NavLinkWrapper from 'components/NavLinkWrapper';
import { Theme } from 'services/ThemeHelper';
import { withContext } from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
require('./DataPrepLink.scss');

interface IDataPrepLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class DataPrepLink extends React.PureComponent<IDataPrepLinkProps> {
  public render() {
    if (Theme.showDataPrep === false) {
      return null;
    }

    const featureName = Theme.featureNames.dataPrep;
    const { namespace, isNativeLink } = this.props.context;
    const dataPrepUrl = `/ns/${namespace}/dataprep`;

    return (
      <li
        id="navbar-preparation"
        className={classnames({
          active: this.isDataPrepActive(),
        })}
      >
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${dataPrepUrl}` : dataPrepUrl}
        >
          {featureName}
        </NavLinkWrapper>
      </li>
    );
  }

  private isDataPrepActive = (location = window.location): boolean => {
    const match = this.props.match;
    if (match && match.isExact) {
      return true;
    }
    const { namespace } = this.props.context;
    const dataprepBasePath = `/cdap/ns/${namespace}/dataprep`;
    const connectionsBasePath = `/cdap/ns/${namespace}/connections`;

    if (
      location.pathname.startsWith(dataprepBasePath) ||
      location.pathname.startsWith(connectionsBasePath)
    ) {
      return true;
    }
    return false;
  };
}

const DataPrepLinkWithContext = withContext(DataPrepLink);

export default DataPrepLinkWithContext;
