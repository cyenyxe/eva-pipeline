package uk.ac.ebi.eva.pipeline.configuration;

import org.springframework.batch.core.job.DefaultJobParametersValidator;

public class GenotypedVcfJobParametersValidator extends DefaultJobParametersValidator {

    public GenotypedVcfJobParametersValidator() {
	super(new String[] { "input.vcf",
		             "input.vcf.id",
		             "input.study.type",
		             "input.study.name",
		             "input.study.id",
		             "output.dir" },
              new String[] {});
    }

}
