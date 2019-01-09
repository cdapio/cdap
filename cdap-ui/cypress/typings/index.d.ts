export {}
declare global {
  namespace Cypress {
    // tslint:disable-next-line: interface-name
    interface Chainable {
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
