/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.pipeline.configuration.validation;

import java.io.File;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobParametersInvalidException;

/**
 * Tests that the arguments necessary to run a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VariantNormalizerStep} are 
 * correctly validated
 */
public class VariantNormalizerStepParametersValidatorTest {

    VariantNormalizerStepParametersValidator validator;
    
    @Before
    public void setUp() {
        validator = new VariantNormalizerStepParametersValidator();
    }
    
    @Test
    public void inputVcfIsValid() throws JobParametersInvalidException {
        validator.validateInputVcf(VariantNormalizerStepParametersValidatorTest.class.getResource(
                "/parameters-validation/input.vcf.gz").getFile());
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfNotExists() throws JobParametersInvalidException {
        validator.validateInputVcf("file://path/to/file.vcf");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfNotReadable() throws JobParametersInvalidException, IOException {
        File file = new File(VariantNormalizerStepParametersValidatorTest.class.getResource(
                "/parameters-validation/input_not_readable.vcf.gz").getFile());
        file.setReadable(false);
        validator.validateInputVcf(file.getCanonicalPath());
    }

    @Test
    public void inputVcfIdValid() throws JobParametersInvalidException {
        validator.validateInputVcfId("id");
        validator.validateInputVcfId("id12345");
        validator.validateInputVcfId("id-12345");
        validator.validateInputVcfId("12345.id");
    }

    @Test(expected = JobParametersInvalidException.class)
    public void inputVcfIdEmpty() throws JobParametersInvalidException {
        validator.validateInputVcfId("");
    }

}
