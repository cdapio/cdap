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
import { DropdownItem } from 'reactstrap';

interface IEntitiesLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class EntitiesLink extends React.PureComponent<IEntitiesLinkProps> {
  public render() {
    const featureName = Theme.featureNames.entities;
    const { namespace, isNativeLink } = this.props.context;
    const baseCDAPUrl = `/ns/${namespace}`;
    return (
      <DropdownItem tag="li">
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${baseCDAPUrl}` : baseCDAPUrl}
          isActive={this.isEntitiesActive}
        >
          {featureName}
        </NavLinkWrapper>
      </DropdownItem>
    );
  }

  private isEntitiesActive = (): boolean => {
    const location = window.location;
    const { namespace } = this.props.context;
    const basePath = `/cdap/ns/${namespace}`;

    const dataprepBasePath = `${basePath}/dataprep`;
    const connectionsBasePath = `${basePath}/connections`;
    const rulesenginepath = `${basePath}/rulesengine`;
    const analytics = `${basePath}/experiments`;
    const namespaceDetails = `${basePath}/details`;
    const createProfile = `${basePath}/profiles/create`;
    const profileDetails = `${basePath}/profiles/details`;
    const dashboardPath = `${basePath}/operations`;
    const reportsPath = `${basePath}/reports`;
    return (
      location.pathname.startsWith(basePath) &&
      !location.pathname.startsWith(dataprepBasePath) &&
      !location.pathname.startsWith(connectionsBasePath) &&
      !location.pathname.startsWith(rulesenginepath) &&
      !location.pathname.startsWith(analytics) &&
      !location.pathname.startsWith(namespaceDetails) &&
      !location.pathname.startsWith(createProfile) &&
      !location.pathname.startsWith(profileDetails) &&
      !location.pathname.startsWith(dashboardPath) &&
      !location.pathname.startsWith(reportsPath)
    );
  };
}

const EntitiesLinkWithContext = withContext(EntitiesLink);

export default EntitiesLinkWithContext;
