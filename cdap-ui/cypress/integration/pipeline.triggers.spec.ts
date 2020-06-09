/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/
import { dataCy } from '../helpers';
import {
    loginIfRequired,
    getArtifactsPoll,
} from '../helpers';
let headers = {};
const TRIGGER_PIPELINE_1 = `trigger_test_pipeline_${Date.now()}`;
const TRIGGER_PIPELINE_2 = `trigger_test_pipeline_2_${Date.now()}`;
describe('Pipeline Studio', () => {
    // Uses API call to login instead of logging in manually through UI
    before(() => {
        loginIfRequired().then(() => {
            cy.getCookie('CDAP_Auth_Token').then((cookie) => {
                if (!cookie) {
                    return;
                }
                headers = {
                    Authorization: 'Bearer ' + cookie.value,
                };
            });
        });
    });

    beforeEach(() => {
        getArtifactsPoll(headers);
    });

    after(() => {
        // Delete the pipeline to clean up
        cy.cleanup_pipelines(headers, TRIGGER_PIPELINE_1);
        cy.cleanup_pipelines(headers, TRIGGER_PIPELINE_2);
    });

    it('Should be rendered correctly', () => {
        cy.visit('/pipelines/ns/default/studio');
    });

    it('Should create first pipeline and deploy it', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.url().should('include', '/studio');
        cy.upload_pipeline(
            'pipeline_with_macros.json',
            '#pipeline-import-config-link > input[type="file"]'
        ).then((subject) => {
            expect(subject.length).to.be.eq(1);
        });
        cy.get('[title="Airport_source"').should('exist');
        cy.get('.pipeline-name').click();
        cy.get('#pipeline-name-input')
            .clear()
            .type(TRIGGER_PIPELINE_1)
            .type('{enter}');
        cy.get(dataCy('deploy-pipeline-btn')).click();
        cy.get(dataCy('Deployed')).should('exist');
    });

    it('Should create second pipeline and deploy it', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.url().should('include', '/studio');
        cy.upload_pipeline(
            'pipeline_with_macros.json',
            '#pipeline-import-config-link > input[type="file"]'
        ).then((subject) => {
            expect(subject.length).to.be.eq(1);
        });
        cy.get('[title="Airport_source"').should('exist');
        cy.get('.pipeline-name').click();
        cy.get('#pipeline-name-input')
            .clear()
            .type(TRIGGER_PIPELINE_2)
            .type('{enter}');
        cy.get(dataCy('deploy-pipeline-btn')).click();
        cy.get(dataCy('Deployed')).should('exist');
    });

    it('Should enable trigger for pipeline2 when pipeline1 succeeds with a simple trigger and disabling it', () => {
        //opening inbound trigger and setting a trigger when pipeline1 succeeds
        cy.get(dataCy('inbound-triggers-toggle')).click();
        cy.get(dataCy('set-triggers-tab')).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).should('exist');
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-expanded`)).get(dataCy('enable-trigger-btn')).click();
        cy.get(dataCy('enabled-triggers-tab')).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).should('exist');
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-expanded`)).get(dataCy('disable-trigger-btn')).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).should('not.exist');
        cy.get(dataCy('inbound-triggers-toggle')).click();
    });

    it('Should enable trigger for pipeline2 when pipeline1 succeeds with a complex trigger',()=>{
        cy.get(dataCy('inbound-triggers-toggle')).click();
        cy.get(dataCy('set-triggers-tab')).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).should('exist');
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-collapsed`)).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_1}-expanded`)).get(dataCy('trigger-config-btn')).click();
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-trigger')} select`).select('source_path');
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-triggered')} select`).select('source_path');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-trigger')} select`).select('sink_path');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-triggered')} select`).select('sink_path');
        cy.get(dataCy('configure-and-enable-trigger-btn')).click();
        cy.get(dataCy('enabled-triggers-tab')).click();
        cy.get(dataCy('view-payload-btn')).click();
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-trigger')} select`).should('be.disabled');
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-triggered')} select`).should('be.disabled');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-trigger')} select`).should('be.disabled');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-triggered')} select`).should('be.disabled');
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-trigger')} select`).should('have.value','source_path');
        cy.get(`${dataCy(`row-0`)} ${dataCy('runtime-arg-of-triggered')} select`).should('have.value','source_path');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-trigger')} select`).should('have.value','sink_path');
        cy.get(`${dataCy(`row-1`)} ${dataCy('runtime-arg-of-triggered')} select`).should('have.value','sink_path');
    })

    it('Should have outbound trigger available in pipeline1', () => {
        cy.visit(`/pipelines/ns/default/view/${TRIGGER_PIPELINE_1}`);
        cy.url().should('include', '/view');
        cy.get(dataCy('outbound-triggers-toggle')).should('exist');
        cy.get(dataCy('outbound-triggers-toggle')).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_2}-triggered-collapsed`)).should('exist');
        cy.get(dataCy(`${TRIGGER_PIPELINE_2}-triggered-collapsed`)).click();
        cy.get(dataCy(`${TRIGGER_PIPELINE_2}-triggered-expanded`)).contains('Succeeds');
    });

});