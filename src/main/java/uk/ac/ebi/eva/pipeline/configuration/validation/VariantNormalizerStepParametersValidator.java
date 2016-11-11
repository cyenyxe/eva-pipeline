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

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Validates the job parameters necessary to execute a {@link uk.ac.ebi.eva.pipeline.jobs.steps.VariantNormalizerStep}
 * 
 * <p>
 * Mandatory arguments are those related to the input VCF (path, ID and aggregation mode), the study (ID, name and type)
 * and the output directory.
 */
public class VariantNormalizerStepParametersValidator extends DefaultJobParametersValidator {

    public VariantNormalizerStepParametersValidator() {
        super(new String[] { JobParametersNames.INPUT_VCF, JobParametersNames.INPUT_VCF_ID,
                             JobParametersNames.INPUT_VCF_AGGREGATION, JobParametersNames.INPUT_STUDY_ID,
                             JobParametersNames.INPUT_STUDY_NAME, JobParametersNames.INPUT_STUDY_TYPE,
                             JobParametersNames.OUTPUT_DIR}, 
              new String[] { });
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);

        validateInputVcf(parameters.getString(JobParametersNames.INPUT_VCF));
        validateInputVcfId(parameters.getString(JobParametersNames.INPUT_VCF_ID));
        // TODO Custom validations
    }

    /**
     * Checks that the input VCF exists and is readable.
     * 
     * @param filePath Path to the input VCF file
     * @throws JobParametersInvalidException If the file is not a valid path, does not exist or is not readable
     */
    void validateInputVcf(String filePath) throws JobParametersInvalidException {
        Path path;
        try {
            path = Paths.get(filePath);
        } catch (InvalidPathException e) {
            throw new JobParametersInvalidException(e.getMessage());
        }

        if (Files.notExists(path)) {
            throw new JobParametersInvalidException("The input VCF file does not exist");
        }

        if (!Files.isReadable(path)) {
            throw new JobParametersInvalidException("The input VCF file is not readable");
        }
    }

    /**
     * Checks that the input VCF identifier has been filled in.
     * 
     * @param id Identifier for the input VCF
     * @throws JobParametersInvalidException If the identifier is empty
     */
    void validateInputVcfId(String id) throws JobParametersInvalidException {
        if (id.isEmpty()) {
            throw new JobParametersInvalidException("A unique file ID must be specified");
        }
    }
}
