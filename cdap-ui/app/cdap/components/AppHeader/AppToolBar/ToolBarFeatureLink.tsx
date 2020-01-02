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
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import ExtendedLinkButton from 'components/AppHeader/ExtendedLinkButton';
import classnames from 'classnames';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Button, { ButtonProps } from '@material-ui/core/Button';

interface IToolBarFeatureLinkProps extends WithStyles<typeof styles> {
  context: INamespaceLinkContext;
  featureFlag: boolean;
  featureName: string;
  featureUrl: string;
}

const styles = (theme) => {
  return {
    buttonLink: {
      ...theme.buttonLink,
      fontWeight: 300,
      color: theme.palette.grey[700],
      padding: `${theme.Spacing(2)}px ${theme.Spacing(3)}px`,
    },
  };
};

class ToolBarFeatureLink extends React.PureComponent<IToolBarFeatureLinkProps> {
  public render() {
    const { classes, featureFlag, featureName, featureUrl } = this.props;
    if (featureFlag === false) {
      return null;
    }
    const { isNativeLink } = this.props.context;
    const Comp = isNativeLink ? 'a' : ExtendedLinkButton(featureUrl);
    let ReactRef;
    // Typescript won't infer if 'isNativeLink' then its an anchor element.
    if (Comp === 'a') {
      ReactRef = React.forwardRef<HTMLElement, 'a'>((props, ref) => <Comp {...props} />);
    } else {
      ReactRef = React.forwardRef<HTMLElement, ButtonProps>((props, ref) => <Comp {...props} />);
    }
    return (
      <Button
        component={ReactRef}
        className={classnames(classes.buttonLink)}
        href={`/cdap${featureUrl}`}
        data-cy={featureName}
      >
        {featureName}
      </Button>
    );
  }
}

const ToolBarFeatureLinkWithContext = withStyles(styles)(withContext(ToolBarFeatureLink));

export default ToolBarFeatureLinkWithContext;
