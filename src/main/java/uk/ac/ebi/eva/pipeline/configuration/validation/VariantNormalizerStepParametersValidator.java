package uk.ac.ebi.eva.pipeline.configuration.validation;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.DefaultJobParametersValidator;

import uk.ac.ebi.eva.pipeline.configuration.JobParametersNames;

/**
 * Validator for job parameters necessary to execute a
 * {@link uk.ac.ebi.eva.pipeline.jobs.steps.VariantNormalizerStep}
 */
public class VariantNormalizerStepParametersValidator extends DefaultJobParametersValidator {

    public VariantNormalizerStepParametersValidator() {
        super(new String[]{ JobParametersNames.INPUT_VCF,
                            JobParametersNames.OUTPUT_DIR,
                            // TODO Probably more... 
                            }, 
              new String[]{});
        // TODO Auto-generated constructor stub
    }

    @Override
    public void validate(JobParameters parameters) throws JobParametersInvalidException {
        super.validate(parameters);
        // TODO Custom validations
    }

}
