/*
 * Copyright © 2019 Cask Data, Inc.
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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Heading, { IHeadingProps } from 'components/Heading';
import ThemeWrapper from 'components/ThemeWrapper';

export const h2Styles = (theme): StyleRules => ({
  root: {
    fontSize: '1.4rem !important',
    fontWeight: 'bold',
    borderBottom: `1px solid ${theme.palette.grey['300']}`,
    paddingBottom: 4,
  },
});

const styles = (theme): StyleRules => {
  /**
   * We set the font sizes here for two reasons,
   *
   * 1. To make headings a little bit easier to read in documentation
   * 2. We have `!important` because we lack specificity here compared to the
   *   rule specified in bootstrap_patch
   */
  return {
    h1Styles: {
      background: 'var(--plugin-reference-heading-bg-color)',
      color: 'white',
      padding: '5px',
      margin: '0 -10px',
      fontSize: '1.5rem !important',
      fontWeight: 'bold',
    },
    h2Styles: h2Styles(theme).root,
    h3Styles: {
      fontSize: '1.3rem !important',
      fontWeight: 600,
    },
    h4Styles: {
      fontSize: '1.2rem !important',
      fontWeight: 500,
    },
    h5Styles: {
      fontSize: '1.1rem !important',
    },
  };
};

interface IMarkdownHeadingProps extends WithStyles<typeof styles>, IHeadingProps {}
const MarkdownHeading: React.SFC<IMarkdownHeadingProps> = ({ classes, ...props }) => {
  const style = classes[`${props.type}Styles`];
  if (typeof window.angular !== 'undefined' && window.angular.version) {
    return (
      <ThemeWrapper>
        <Heading {...props} className={`${props.className} ${style}`} />
      </ThemeWrapper>
    );
  }
  return <Heading {...props} className={`${props.className} ${style}`} />;
};
const MarkdownHeadingWithStyles = withStyles(styles)(MarkdownHeading);
export { MarkdownHeadingWithStyles as MarkdownHeading };
