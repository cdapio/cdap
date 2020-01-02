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
import Heading, { HeadingTypes } from 'components/Heading';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { isNilOrEmptyString, objectQuery } from 'services/helpers';
const styles = (theme) => {
  return {
    featureHeading: {
      display: 'inline-block',
      verticalAlign: 'middle',
      margin: '0',
      borderLeft: `2px solid ${theme.palette.grey[400]}`,
      /**
       *  important? wat?
       * 1. Its ok to have important here as we maintain it in single file and we don't
       *   want any other component to overwrite this at any point
       * 2. We right now have this weird transition state where our bootstrap htag
       *    specs are a little different to the new material spec we are trying to adapt to.
       *
       */
      fontSize: `${theme.typography.h4FontSize} !important`,
      paddingLeft: '10px',
      color: 'var(--page-name-color)',
      lineHeight: '1',
      fontWeight: 300,
    },
  };
};
interface IFeatureHeadingState {
  featureName: string;
}

class FeatureHeading extends React.PureComponent<WithStyles, IFeatureHeadingState> {
  public state: IFeatureHeadingState = {
    featureName: '',
  };

  private mutationObserver: MutationObserver = new MutationObserver(
    (mutations: MutationRecord[]) => {
      if (!Array.isArray(mutations) || mutations.length === 0) {
        return;
      }
      const title = objectQuery(mutations[0], 'target', 'textContent');
      if (isNilOrEmptyString(title)) {
        return;
      }
      this.setState({
        featureName: (objectQuery(title.split('|'), 1) || '').trim(),
      });
    }
  );

  private mutationObserverConfig = { childList: true, subtree: true };

  constructor(props) {
    super(props);
    this.mutationObserver.observe(document.querySelector('title'), this.mutationObserverConfig);
  }
  public componentWillUnmount() {
    this.mutationObserver.disconnect();
  }
  public render() {
    return (
      <Heading
        data-cy="feature-heading"
        type={HeadingTypes.h4}
        label={this.state.featureName}
        className={this.props.classes.featureHeading}
      />
    );
  }
}

export default withStyles(styles)(FeatureHeading);
