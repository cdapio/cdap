/*
 * Copyright © 2020 Cask Data, Inc.
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
import Select from 'components/AbstractWidget/FormInputs/Select';
import { SchemaEditor, heightOfRow } from 'components/AbstractWidget/SchemaEditor';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import ee from 'event-emitter';
import { getDefaultEmptyAvroSchema } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import PropTypes from 'prop-types';
import LoadingSVG from 'components/LoadingSVG';
import If from 'components/If';
import Textbox from 'components/AbstractWidget/FormInputs/TextBox';
import { RefreshableSchemaEditor } from 'components/PluginSchemaEditor/RefreshableSchemaEditor';
import ConfigurableTab from 'components/ConfigurableTab';
import classnames from 'classnames';
import { objectQuery, isNilOrEmptyString } from 'services/helpers';
import Alert from 'components/Alert';
import { isObject } from 'vega-lite/build/src/util';
import { isMacro } from 'services/helpers';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import isEqual from 'lodash/isEqual';
import { isNoSchemaAvailable } from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';

const styles = (theme): StyleRules => {
  return {
    container: {
      display: 'block',
      height: '100%',
    },
    fieldset: {
      '&[disabled] *': {
        color: `${theme.palette.grey[200]}`,
        cursor: 'not-allowed !important',
      },
    },
    tabContent: {
      width: '100%',
    },
    header: {
      display: 'grid',
      gridTemplateColumns: 'auto 100px',
      alignItems: 'center',
      margin: '-15px -10px 5px -10px',
      padding: '0 10px 5px;',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
    },
    disabledHeader: {
      margin: '0 -10px 5px -10px',
      padding: '0 10px 10px',
    },
    title: {
      fontWeight: 500,
    },
    actionsDropdown: {
      border: `1px solid ${theme.palette.grey[400]}`,
      borderRadius: '4px',
    },
    loadingContainer: {
      textAlign: 'center',
    },
    macroTextBox: {
      width: '100%',
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      padding: '5px',
    },
  };
};

enum SchemaActionsEnum {
  IMPORT = 'import',
  EXPORT = 'export',
  CLEAR = 'clear',
  MACRO = 'macro',
  EDITOR = 'editor',
  PROPAGATE = 'propagate',
}
enum IPluginSchemaEditorModes {
  Macro = 'macro',
  Editor = 'editor',
}
interface IActionsOptionsObj {
  disabled?: boolean;
  tooltip?: string;
  label: string;
  value: SchemaActionsEnum;
  onClick?: () => void;
}
interface IPluginSchema {
  name: string;
  schema: string;
}
type IActionsDropdownTooltip = Record<SchemaActionsEnum, IActionsOptionsObj>;
interface IPluginSchemaEditorState {
  error: string;
  schemas: IPluginSchema[];
  mode: IPluginSchemaEditorModes;
  loading: boolean;
  schemaRowCount: number;
}

interface IPluginSchemaEditorProps extends WithStyles<typeof styles> {
  disabled?: boolean;
  actionsDropdownMap?: IActionsDropdownTooltip;
  schemas: IPluginSchema[];
  onSchemaChange?: (schemas: IPluginSchema[]) => void;
  schemaTitle?: string;
  isSchemaMacro?: boolean;
  errors?: Record<string, Record<string, string>>;
}

const EXPERIMENT_ID = 'schema-editor';

class PluginSchemaEditorBase extends React.PureComponent<
  IPluginSchemaEditorProps,
  IPluginSchemaEditorState
> {
  private actions: IActionsOptionsObj[] =
    this.props.actionsDropdownMap &&
    Object.values(this.props.actionsDropdownMap).map((value) => value);
  private containerRef;

  private getMode = () => {
    if (this.props.disabled) {
      return isMacro(objectQuery(this.props.schemas, 0, 'schema') || '')
        ? IPluginSchemaEditorModes.Macro
        : IPluginSchemaEditorModes.Editor;
    }
    return this.props.isSchemaMacro
      ? IPluginSchemaEditorModes.Macro
      : IPluginSchemaEditorModes.Editor;
  };

  public state = {
    error: null,
    schemas: this.props.schemas,
    loading: false,
    mode: this.getMode(),
    schemaRowCount: null,
  };

  private ee = ee(ee);

  constructor(props) {
    super(props);

    const isExperimentEnabled = window.localStorage.getItem(EXPERIMENT_ID) === 'true';

    if (!this.props.disabled && isExperimentEnabled) {
      this.ee.on('schema.import', this.onSchemaImport);
      this.ee.on('dataset.selected', this.onSchemaImport);
      if (window.Cypress) {
        window.Cypress.cy.on('schema.import', this.onSchemaImport);
        window.Cypress.cy.on('dataset.selected', this.onSchemaImport);
      }
    }
    let doesActionDropdownHasExport;
    if (this.props.actionsDropdownMap && isExperimentEnabled) {
      doesActionDropdownHasExport = Object.keys(this.props.actionsDropdownMap).find(
        (key) => key.toLowerCase() === 'export'
      );
    }
    if (doesActionDropdownHasExport) {
      // Schema can be exported on detailed view even if it is disabled.
      this.ee.on('schema.export', this.onSchemaExport);
      if (window.Cypress) {
        window.Cypress.cy.on('schema.export', this.onSchemaExport);
      }
    }
    window.addEventListener('resize', this.calculateSchemaRowCount);
  }

  public componentWillReceiveProps(nextProps: IPluginSchemaEditorProps) {
    if (!nextProps.actionsDropdownMap || !Object.keys(nextProps.actionsDropdownMap).length) {
      this.actions = [];
      return;
    }
    this.actions = Object.values(nextProps.actionsDropdownMap).map((value) => value);
    return;
  }

  public shouldComponentUpdate(nextProps: IPluginSchemaEditorProps, nextState) {
    const { disabled, isSchemaMacro, actionsDropdownMap, schemas, errors } = nextProps;
    const { schemaRowCount, loading, mode, error } = nextState;
    const newActions =
      actionsDropdownMap &&
      Object.values(actionsDropdownMap)
        .filter((value) => typeof value === 'string')
        .join('--');
    const existingActions =
      this.props.actionsDropdownMap &&
      Object.values(this.props.actionsDropdownMap)
        .filter((value) => typeof value === 'string')
        .join('--');
    let existingSchemas;
    if (typeof this.props.schemas === 'string') {
      existingSchemas = this.props.schemas;
    } else {
      existingSchemas = this.props.schemas.map((s) => s.schema).join('__');
    }
    let newSchemas;
    if (typeof schemas === 'string') {
      newSchemas = schemas;
    } else {
      newSchemas = schemas.map((s) => s.schema).join('__');
    }

    const didPropsChange =
      disabled !== this.props.disabled ||
      isSchemaMacro !== this.props.isSchemaMacro ||
      newActions !== existingActions ||
      newSchemas !== existingSchemas ||
      !isEqual(errors, this.props.errors);

    const didStateChange =
      schemaRowCount !== this.state.schemaRowCount ||
      loading !== this.state.loading ||
      mode !== this.state.mode ||
      error !== this.state.error;
    return didStateChange || didPropsChange;
  }

  public componentDidMount() {
    this.calculateSchemaRowCount();
  }

  public componentWillUnmount() {
    this.ee.off('schema.import', this.onSchemaImport);
    this.ee.off('schema.export', this.onSchemaExport);
    this.ee.off('dataset.selected', this.onSchemaImport);
    window.removeEventListener('resize', this.calculateSchemaRowCount);
  }

  public calculateSchemaRowCount = () => {
    if (this.containerRef) {
      const { height } = this.containerRef.getBoundingClientRect();
      let schemaRowCount = Math.floor(height / heightOfRow) - 1;
      // For sinks we can't determine the height. So set it to by default 20 (20 * 34 = 680px in height)
      if (schemaRowCount < 5) {
        schemaRowCount = 20;
      }
      this.setState({ schemaRowCount });
    }
  };

  public onSchemaExport = () => {
    let schemasToExport;
    if (typeof this.props.schemas === 'string') {
      try {
        schemasToExport = {
          name: 'etlSchemaBody',
          schema: JSON.parse(this.props.schemas),
        };
      } catch (e) {
        schemasToExport = this.props.schemas;
      }
    } else {
      schemasToExport = this.props.schemas.map((schema) => {
        try {
          return {
            name: schema.name,
            schema: JSON.parse(schema.schema),
          };
        } catch (e) {
          return schema;
        }
      });
    }
    // // CDAP-17106 - Need to use generic DownloadFile function here from download-file
    const blob = new Blob([JSON.stringify(schemasToExport, null, 4)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const exportFileName = 'schema';
    const a = document.createElement('a');
    a.href = url;
    a.download = `${exportFileName}.json`;
    if (window.Cypress) {
      return;
    }

    const clickHandler = (event) => {
      event.stopPropagation();
    };
    a.addEventListener('click', clickHandler, false);
    a.click();
  };

  private onSchemaImport = (schemas) => {
    if (this.state.mode === IPluginSchemaEditorModes.Macro) {
      return;
    }
    let importedSchemas = schemas;
    if (typeof schemas === 'string') {
      try {
        importedSchemas = JSON.parse(schemas);
      } catch (e) {
        this.setState({
          error: e.message,
        });
        return;
      }
    }
    let hasError = false;
    if (!Array.isArray(importedSchemas)) {
      // This will be the case when we hit 'Apply' button from wrangler.
      // We don't maintain consistency in importing schemas from multiple
      // sources. This will take time to fix as we right now throw schemas
      // in multiple formats across all places.
      if (isObject(importedSchemas) && !importedSchemas.hasOwnProperty('schema')) {
        importedSchemas = { name: 'etlSchemaBody', schema: importedSchemas };
      }
      importedSchemas = [importedSchemas];
    }
    const newSchemas = importedSchemas.map((schema) => {
      if (typeof schema !== 'object' && schema.hasOwnProperty('schema')) {
        return { ...getDefaultEmptyAvroSchema() };
      }
      const s = { ...schema };
      try {
        if (typeof schema.schema === 'string') {
          s.schema = JSON.parse(schema.schema);
        }
      } catch (e) {
        this.setState({
          error: e.message,
        });
        hasError = true;
        return;
      }
      if (!s.schema || s.schema.type !== 'record' || !Array.isArray(s.schema.fields)) {
        this.setState({
          error: 'Imported schema is not a valid Avro schema',
        });
        hasError = true;
        return;
      }
      if (s.schema.name) {
        s.schema.name = s.schema.name.replace('.', '.type');
      }
      return s;
    });
    if (hasError) {
      return;
    }
    this.setState({ loading: true }, () => {
      const schemasForPlugin = newSchemas.map((s) => {
        if (typeof s.schema !== 'string') {
          s.schema = JSON.stringify(s.schema);
        }
        return s;
      });
      if (typeof this.props.onSchemaChange === 'function') {
        this.props.onSchemaChange(schemasForPlugin);
      }
      setTimeout(() => {
        this.setState({
          loading: false,
        });
      }, 1000);
    });
  };

  private onActionsHandler = (value) => {
    const specificAction = this.actions.find((action) => action.value === value);
    if (!specificAction) {
      return;
    }
    specificAction.onClick();
    if (value === SchemaActionsEnum.CLEAR) {
      specificAction.onClick();
      this.setState({ loading: true }, () => {
        setTimeout(() => {
          this.setState({
            loading: false,
          });
        }, 1000);
      });
    } else if (value === SchemaActionsEnum.MACRO) {
      const newState = {
        mode:
          this.state.mode === IPluginSchemaEditorModes.Editor
            ? IPluginSchemaEditorModes.Macro
            : IPluginSchemaEditorModes.Editor,
        schemas:
          this.state.mode === IPluginSchemaEditorModes.Editor
            ? [{ name: 'etlSchemaBody', schema: '${}' }]
            : [{ name: 'etlSchemaBody', schema: '' }],
      };
      this.setState({ mode: newState.mode });
      if (typeof this.props.onSchemaChange === 'function') {
        this.props.onSchemaChange(newState.schemas);
      }
    }
  };

  private santizeSchemasForEditor = (schemas = this.props.schemas): ISchemaType[] => {
    if (typeof schemas === 'string') {
      let returnSchema;
      try {
        returnSchema = JSON.parse(schemas);
      } catch (e) {
        return [{ ...getDefaultEmptyAvroSchema() }];
      }
      if (!returnSchema.schema) {
        return [
          {
            name: 'etlSchemaBody',
            schema: !returnSchema.fields.length ? getDefaultEmptyAvroSchema() : returnSchema,
          },
        ];
      }
      return [returnSchema];
    }
    return (schemas || []).map((s) => {
      const newSchema = {
        name: s.name,
        schema: null,
      };
      if (typeof s.schema === 'string') {
        try {
          newSchema.schema = JSON.parse(s.schema);
        } catch (e) {
          return { ...getDefaultEmptyAvroSchema() };
        }
      } else {
        newSchema.schema = s.schema;
      }
      if (isNoSchemaAvailable(newSchema)) {
        newSchema.schema = `No ${this.props.schemaTitle} available`;
      }
      return newSchema;
    });
  };

  public renderSchemaIntabs = () => {
    const tabs = this.santizeSchemasForEditor().map((s, i) => {
      let content = (
        <fieldset
          disabled={this.props.disabled}
          className={this.props.classes.fieldset}
          key={i}
          data-cy="schema-editor-fieldset-container"
        >
          <RefreshableSchemaEditor
            visibleRows={this.state.schemaRowCount}
            schema={s}
            disabled={this.props.disabled}
            onChange={({ avroSchema }) => {
              const newSchemas = [...this.props.schemas];
              newSchemas[i] = {
                name: avroSchema.name,
                schema: JSON.stringify(avroSchema.schema),
              };
              if (typeof this.props.onSchemaChange === 'function') {
                this.props.onSchemaChange(newSchemas);
              }
            }}
            errors={
              this.props.errors && this.props.errors[s.name] ? this.props.errors[s.name] : null
            }
          />
        </fieldset>
      );
      if (typeof s.schema === 'string') {
        content = s.schema;
      }
      return {
        id: i,
        name: s.name,
        content,
        contentClassName: this.props.classes.tabContent,
        paneClassName: this.props.classes.tabContent,
      };
    });
    const tabProps = {
      activeTab: 0,
      tabConfig: {
        tabs,
        layout: 'horizontal',
        defaultTab: 0,
      },
    };
    return <ConfigurableTab activeTab={tabProps.activeTab} tabConfig={tabProps.tabConfig} />;
  };

  public renderSchemaEditors = () => {
    if (
      this.state.loading ||
      this.state.mode === IPluginSchemaEditorModes.Macro ||
      !this.state.schemaRowCount
    ) {
      return null;
    }
    if (Array.isArray(this.props.schemas) && this.props.schemas.length > 1) {
      return this.renderSchemaIntabs();
    }
    if (this.props.disabled && !this.props.schemas.length) {
      return `No ${this.props.schemaTitle} available`;
    }
    let incomingSchemas = this.props.schemas;
    /**
     * There are cases where plugins can define no schema to be a valid schema.
     * In this case we only want show 'No schema available' when the plugin developer
     * is using a non-editable-schema-editor widget.
     */
    if (incomingSchemas.length === 1 && isNoSchemaAvailable(incomingSchemas[0])) {
      if (this.props.disabled) {
        return `No ${this.props.schemaTitle} available`;
      } else {
        // For all other plugins if there is no schema and if it is not disabled show default empty schema.
        incomingSchemas = [
          { name: 'etlSchemaBody', schema: JSON.stringify(getDefaultEmptyAvroSchema()) },
        ];
      }
    }

    return this.santizeSchemasForEditor(incomingSchemas).map((schema, i) => (
      <fieldset
        disabled={this.props.disabled}
        className={this.props.classes.fieldset}
        data-cy="schema-editor-fieldset-container"
      >
        <SchemaEditor
          key={i}
          visibleRows={this.state.schemaRowCount}
          schema={schema}
          disabled={this.props.disabled}
          onChange={({ avroSchema }) => {
            let newSchemas = [...this.props.schemas];
            if (typeof this.props.schemas === 'string') {
              const sanitizedSchema = this.santizeSchemasForEditor();
              newSchemas = [
                {
                  name: 'etlSchemaBody',
                  schema: sanitizedSchema.length ? JSON.stringify(sanitizedSchema[0].schema) : '',
                },
              ];
            }
            const fields = objectQuery(avroSchema, 'schema', 'fields');
            if (!Array.isArray(fields) || !fields.length) {
              newSchemas[i] = { ...getDefaultEmptyAvroSchema(), schema: '' };
            } else {
              newSchemas[i] = avroSchema;
            }
            newSchemas[i].schema = JSON.stringify(newSchemas[i].schema);
            if (typeof this.props.onSchemaChange === 'function') {
              this.props.onSchemaChange(newSchemas);
            }
          }}
          errors={
            this.props.errors && this.props.errors[schema.name]
              ? objectQuery(this.props, 'errors', schema.name)
              : objectQuery(this.props, 'errors', 'noSchemaSection') || null
          }
        />
      </fieldset>
    ));
  };

  public renderMacroEditor = () => {
    if (
      this.state.loading ||
      this.state.mode === IPluginSchemaEditorModes.Editor ||
      !this.state.schemaRowCount
    ) {
      return null;
    }
    const macro = this.props.schemas[0].schema;
    if (this.props.disabled) {
      return macro;
    }
    return (
      <Textbox
        disabled={this.props.disabled}
        value={macro}
        className={this.props.classes.macroTextBox}
        onChange={(value) => {
          const newSchemas = [{ name: 'etlSchemaBody', schema: value }];
          if (typeof this.props.onSchemaChange === 'function') {
            this.props.onSchemaChange(newSchemas);
          }
        }}
        dataCy={`${this.props.schemaTitle}-macro-input`}
      />
    );
  };

  public renderHeader = () => {
    const { classes, schemaTitle, disabled } = this.props;
    return (
      <div
        className={classnames(classes.header, {
          [classes.disabledHeader]: disabled,
        })}
      >
        <div className={classes.title}>{schemaTitle || 'Schema'}</div>
        <If condition={this.actions && this.actions.length > 0}>
          <Select
            classes={{ root: classes.actionsDropdown }}
            value={''}
            placeholder="Actions"
            onChange={this.onActionsHandler}
            widgetProps={{
              options: this.actions,
              dense: true,
              MenuProps: {
                MenuListProps: {
                  'data-cy': 'schema-actions-dropdown-menu-list',
                } as any,
              },
            }}
            dataCy="schema-actions-dropdown"
          />
        </If>
      </div>
    );
  };

  public render() {
    const { classes } = this.props;
    return (
      <div
        className={classes.container}
        ref={(ref) => (this.containerRef = ref)}
        data-cy={`${this.props.schemaTitle}`}
      >
        {this.renderHeader()}
        <If condition={this.state.loading}>
          <div className={classes.loadingContainer}>
            <LoadingSVG />
          </div>
        </If>
        <Alert
          showAlert={!isNilOrEmptyString(this.state.error)}
          message={this.state.error}
          type="error"
          onClose={() => {
            this.setState({ error: null });
          }}
        />
        {this.renderSchemaEditors()}
        {this.renderMacroEditor()}
      </div>
    );
  }
}
const StyledPluginSchemaEditor = withStyles(styles)(PluginSchemaEditorBase);

const PluginSchemaEditor = (props) => {
  return (
    <ThemeWrapper>
      <StyledPluginSchemaEditor {...props} />
    </ThemeWrapper>
  );
};
(PluginSchemaEditor as any).propTypes = {
  schemas: PropTypes.array,
  onSchemaChange: PropTypes.func,
  disabled: PropTypes.func,
  actionsDropdownMap: PropTypes.array,
  schemaTitle: PropTypes.string,
  isSchemaMacro: PropTypes.func,
  errors: PropTypes.object,
};
export { PluginSchemaEditor };
