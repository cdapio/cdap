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
require('./ExploreDatasetLink.scss');

interface IExploreDatasetProps {
  context: {
    namespace: string;
  };
}

class ExploreDatasetLink extends React.PureComponent<IExploreDatasetProps> {
  public render() {
    const featureName = "Explore Dataset";
    const { namespace } = this.props.context;

    return (
      <li
        id="navbar-fe"
        className={classnames({
          active: this.isExploreDataset(),
        })}
      onClick = {this.navigateToFeature.bind(this, namespace)}>
      {featureName}
      </li>
    );
  }

  protected navigateToFeature = (namespace) => {
    const feURL = `/ns/${namespace}/exploredataset`;
    const fePath = `/cdap${feURL}`;
    window.location.href = fePath;
  }

  protected isExploreDataset = (location = window.location): boolean => {
    return (location.pathname.indexOf("exploredataset") >= 0);
  }
}

const ExploreDatasetWithContext = withContext(ExploreDatasetLink);

export default ExploreDatasetWithContext;
