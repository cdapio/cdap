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
import {withContext} from 'components/Header/NamespaceLinkContext';
import classnames from 'classnames';
import { Theme } from 'services/ThemeHelper';
require('./FELink.scss');

interface IFELinkProps {
  context: {
    namespace: string;
  };
}

class FELink extends React.PureComponent<IFELinkProps> {
  public render() {
    if (Theme.showFeatureEngineering === false) {
      return null;
    }
    const featureName = "Feature Engineering";
    const { namespace } = this.props.context;

    return (
      <li
        id="navbar-fe"
        className={classnames({
          active: this.isFEActive(),
        })}
      onClick = {this.navigateToFeature.bind(this, namespace)}>
      {featureName}
      </li>
    );
  }

  protected navigateToFeature = (namespace) => {
    const feURL = `/ns/${namespace}/featureEngineering`;
    const fePath = `/cdap${feURL}`;
    window.location.href = window.knoxPrefix + fePath;
  }

  protected isFEActive = (location = window.location): boolean => {
    return (location.pathname.indexOf("featureEngineering") >= 0);
  }
}

const FELinkWithContext = withContext(FELink);

export default FELinkWithContext;
