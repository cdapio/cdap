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
import {withContext} from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
require('./FELink.scss');

interface IFELinkProps {
  context: {
    namespace: string;
  };
}

class FELink extends React.PureComponent<IFELinkProps> {
  public render() {
    if (Theme.showDataPrep === false) {
      return null;
    }

    const featureName = "Feature Engineering";
    const { namespace } = this.props.context;
    const feURL = `/ns/${namespace}/featureEngineering`;

    return (
      <li
        id="navbar-fe"
        className={classnames({
          active: this.isFEActive(),
        })}
      >
        <NavLinkWrapper
          isNativeLink={false}
          to={feURL}
        >
          {featureName}
        </NavLinkWrapper>
      </li>
    );
  }

  protected isFEActive = (location = window.location): boolean => {
    return (location.pathname.indexOf("featureEngineering") >= 0);
  }
}

const FELinkWithContext = withContext(FELink);

export default FELinkWithContext;
