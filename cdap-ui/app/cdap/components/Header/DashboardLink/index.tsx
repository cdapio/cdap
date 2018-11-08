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
import { DropdownItem } from 'reactstrap';

interface IDashboardLinkProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
  match: {
    isExact: boolean;
  };
}

class DashboardLink extends React.PureComponent<IDashboardLinkProps> {
  public render() {
    if (Theme.showDashboard === false) {
      return null;
    }

    const featureName = Theme.featureNames.dashboard;
    const { namespace, isNativeLink } = this.props.context;
    const dashboardUrl = `/ns/${namespace}/operations`;
    return (
      <DropdownItem tag="li">
        <NavLinkWrapper
          isNativeLink={isNativeLink}
          to={isNativeLink ? `/cdap${dashboardUrl}` : dashboardUrl}
          isActive={this.isDashboardActive}
        >
          {featureName}
        </NavLinkWrapper>
      </DropdownItem>
    );
  }

  private isDashboardActive = (): boolean => {
    const match = this.props.match;
    if (match && match.isExact) {
      return true;
    }
    const location = window.location;
    const { namespace } = this.props.context;
    const path = `/cdap/ns/${namespace}/operations`;

    return location.pathname.startsWith(path);
  };
}

const DashboardLinkWithContext = withContext(DashboardLink);

export default DashboardLinkWithContext;
