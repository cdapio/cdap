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
import 'ace-builds/src-min-noconflict/ace';
import ThemeWrapper from 'components/ThemeWrapper';
import PropTypes from 'prop-types';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Button from '@material-ui/core/Button';
import If from 'components/If';
import debounce from 'lodash/debounce';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'block',
      position: 'relative',
      border: `1px solid ${theme.palette.grey[400]}`,
    },
    button: {
      position: 'absolute',
      right: 0,
      top: 0,
      zIndex: 1000,
      margin: 0,
    },
  };
};

export interface IBaseCodeEditorProps {
  mode?: string;
  value: string;
  onChange: (value: string) => void;
  rows?: number;
  tabSize?: number;
  className?: string;
  disabled?: boolean;
  activeLineMarker?: boolean;
  showPrettyPrintButton?: boolean;
  prettyPrintFunction?: (value: string) => string;
  classes: Record<string, string>;
  dataCy?: string;
}

interface ICodeEditorProps extends IBaseCodeEditorProps, WithStyles<typeof styles> {}

class CodeEditorView extends React.Component<ICodeEditorProps> {
  public static LINE_HEIGHT = 20;
  public static defaultProps = {
    mode: 'plain_text',
    value: '',
    rows: 5,
    disabled: false,
    tabSize: 2,
    activeLineMarker: true,
    showPrettyPrintButton: false,
  };
  public aceRef: HTMLElement;
  private editor;

  public componentWillReceiveProps(nextProps) {
    const currentValue = this.editor.getSession().getValue();
    if (nextProps.value === currentValue) {
      return;
    }
    this.editor.getSession().setValue(nextProps.value);
  }

  public componentDidMount() {
    window.ace.config.set('basePath', '/assets/bundle/ace-editor-worker-scripts/');
    this.editor = window.ace.edit(this.aceRef);
    this.editor.getSession().setMode(`ace/mode/${this.props.mode}`);
    this.editor.getSession().setUseWrapMode(true);
    this.editor.getSession().setOptions({ tabSize: this.props.tabSize });
    this.editor.setHighlightActiveLine(this.props.activeLineMarker);
    const textArea = this.editor.textInput.getElement();
    if (textArea) {
      textArea.setAttribute('data-cy', this.props.dataCy);
    }
    if (this.props.disabled) {
      this.editor.setReadOnly(true);
    }

    // TODO: Investigate why this change event is fired multiple times
    // https://issues.cask.co/browse/CDAP-16477
    this.editor.getSession().on('change', this.debouncedChangeHandler);
    this.editor.setShowPrintMargin(false);
  }

  private valueChangeHandler = () => {
    if (typeof this.props.onChange === 'function') {
      const value = this.editor.getSession().getValue();
      this.props.onChange(value);
    }
  };

  private debouncedChangeHandler = debounce(this.valueChangeHandler, 500);

  public shouldComponentUpdate() {
    return false;
  }

  // This componentWillUnmount is to handle the edge case where the user types something
  // and hit escape before the 500ms debounce have cleared. So to guarantee the onChange
  // to fire before the modal closes, the debounce is cancelled and the onChange is called
  // manually
  public componentWillUnmount() {
    this.debouncedChangeHandler.cancel();
    this.valueChangeHandler();
  }

  public render() {
    const { value, className, classes } = this.props;
    return (
      <div className={classes.root}>
        <div
          className={`${className}`}
          style={{ height: `${this.props.rows * CodeEditorView.LINE_HEIGHT}px` }}
          ref={(ref) => (this.aceRef = ref)}
        >
          {value}
        </div>
        <If condition={this.props.showPrettyPrintButton}>
          <Button
            className={classes.button}
            variant="outlined"
            onClick={() => {
              let code = this.editor.getSession().getValue();
              if (typeof this.props.prettyPrintFunction === 'function') {
                code = this.props.prettyPrintFunction(code);
              }
              this.props.onChange(code);
            }}
          >
            Tidy
          </Button>
        </If>
      </div>
    );
  }
}
const StyledCodeEditor = withStyles(styles)(CodeEditorView);
export default function CodeEditor(props) {
  return (
    <ThemeWrapper>
      <StyledCodeEditor {...props} />
    </ThemeWrapper>
  );
}

(CodeEditor as any).propTypes = {
  mode: PropTypes.string,
  value: PropTypes.string,
  onChange: PropTypes.func,
  rows: PropTypes.number,
  disabled: PropTypes.bool,
  tabSize: PropTypes.number,
  activeLineMarker: PropTypes.bool,
  showPrettyPrintButton: PropTypes.bool,
  dataCy: PropTypes.string,
};
