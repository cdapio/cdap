export { }
export interface INodeInfo {
  nodeName: string;
  nodeType: string;
}
export interface INodeIdentifier extends INodeInfo {
  nodeId: string;
}
export interface IgetNodeIDOptions {
  [key: string]: any
}

declare global {
  namespace Cypress {
    // tslint:disable-next-line: interface-name
    interface Chainable {

      open_source_panel: () => Chainable<JQuery<HTMLElement>>;
      open_transform_panel: () => Chainable<JQuery<HTMLElement>>;
      open_analytics_panel: () => Chainable<JQuery<HTMLElement>>;
      open_sink_panel: () => Chainable<JQuery<HTMLElement>>;
      open_condition_and_actions_panel: () => Chainable<JQuery<HTMLElement>>;

      /**
       * Given the nodeidentifier object returns the html element of the plugin
       *
       * @node - NodeIdentifier object to identify the node in the canvas
       */
      get_node: (nodeObj: INodeIdentifier) => Chainable<JQuery<HTMLElement>>;
      /**
       * Add node to canvas
       *
       * @nodeObj - NodeInfo object to identify the node in the side panel
       */
      add_node_to_canvas: (nodeObj: INodeInfo) => Chainable<JQuery<HTMLElement>>;
      /**
       * Get all elements at once for matching selectors
       *
       * @elements - Any number of selectors to search dom
       */
      getAll: (selectors: Array<INodeInfo>) => Chainable<Array<JQuery<HTMLElement>>>;

      /**
       * Connect two plugins. This involves connecting source endpoint to anywhere in the target node.
       *
       * @sourceNode - Source node
       * @targetNode - Target node
       * @sourceEndpoint - Source jsplumb endpoint to connect from the source node. It could be anything,
       * - endpoint on right for transform,
       * - bottom for condition node or error/alert ports
       * - in a popover for splitter plugins
       */
      connect_two_nodes: (sourceNode: INodeIdentifier, targetNode: INodeIdentifier, getSourceNodeId: (options: IgetNodeIDOptions, s: string) => string, options?: IgetNodeIDOptions) => Chainable<any>;
      /**
       * Moves a node based on selector to X,Y co-ordinates
       *
       * @nodeName - Name of the node to move
       * @nodeType - Type of the node
       * @nodeId - unique identifier to indentify the node,
       * @toX - Position X to move the node
       * @toY - Position Y to move the node
       *
       * The final selector that will be generated to find the node will be of the form
       * [data-cy="plugin-node-${nodeName}-${nodeType}-${nodeId}"]
       * */
      move_node: (nodeObj: INodeIdentifier | string, toX: number, toY: number) => Chainable<JQuery<any>>;
      /**
       * Uploads a pipeline json from fixtures to input file element.
       *
       * @fileName - Name of the file from fixture folder including extension.
       * @selector - css selector to query for the input[type="file"] element.
      */
      upload_pipeline: (filename: string, selector: string) => Chainable<JQuery<HTMLElement>>;
      /**
       * Cleans up pipelines once expecuting a specific test. This is to remove state
       *  from individual tests.
       *
       * @headers - Any request headers to be passed.
       * @pipelineName - name of the pipeline to be deleted.
      */
      cleanup_pipelines: (headers: any, pipelineName: string) => Chainable<Request>;

      /**
       * Command to check if wrangler is up and running and if not starts wrangler
       * before running any wrangler related tests.
       */
      start_wrangler: (headers: any) => Chainable<any>;

      /**
       * Fills up the create connection form for GCS
       */

      fill_GCS_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test GCS connection
       */
      test_GCS_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a GCS connection
       */
      create_GCS_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;


      /**
      * Fills up the create connection form for BIGQUERY
      */

      fill_BIGQUERY_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test BIGQUERY connection
       */
      test_BIGQUERY_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a BigQuery connection
       */

      create_BIGQUERY_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;


      /**
      * Fills up the create connection form for BIGQUERY
      */

      fill_SPANNER_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test BIGQUERY connection
       */
      test_SPANNER_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a BigQuery connection
       */

      create_SPANNER_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;
      compareSnapshot: (s: string) => any;
    }
    // tslint:disable-next-line: interface-name
    interface Window {
      File: any;
      DataTransfer: any;
      sessionStorage: any;
      onbeforeunload: any;
    }
  }
}
