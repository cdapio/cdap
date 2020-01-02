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
import PropTypes from 'prop-types';
import React, { createElement } from 'react';
import marksy from 'marksy';
import ThemeWrapper from 'components/ThemeWrapper';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { MarkdownToReactMapping } from 'components/Markdown/MarkdownToReactMapping';
import prism from 'prismjs';
import 'prismjs/components/prism-json.js';
import 'prismjs/themes/prism.css';

const styles = () => {
  return {
    markdownRoot: {
      fontSize: '14px',
    },
  };
};
interface IMarkdownProps extends WithStyles<typeof styles> {
  markdown: string;
}

const Markdown: React.SFC<IMarkdownProps> = ({ markdown, classes }) => {
  const compile = marksy({
    createElement,
    elements: MarkdownToReactMapping,
    highlight(language, code) {
      if (!prism.languages[language]) {
        return code;
      }
      return prism.highlight(code, prism.languages[language]);
    },
  });
  const compiled = compile(markdown);
  return <div className={classes.markdownRoot}>{compiled.tree}</div>;
};

function MarkdownWithStyles({ markdown }) {
  const MDWithStyles = withStyles(styles)(Markdown);
  if (!markdown) {
    return null;
  }
  if (typeof window.angular !== 'undefined' && window.angular.version) {
    return (
      <ThemeWrapper>
        <MDWithStyles markdown={markdown} />
      </ThemeWrapper>
    );
  }
  return <MDWithStyles markdown={markdown} />;
}

// purely for ng-react
(MarkdownWithStyles as any).propTypes = {
  classes: PropTypes.object,
  markdown: PropTypes.string,
};
export { MarkdownWithStyles };
