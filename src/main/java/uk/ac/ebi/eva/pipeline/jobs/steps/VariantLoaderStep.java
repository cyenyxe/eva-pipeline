/*
 * Copyright 2015 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import java.net.URI;
import java.nio.file.Path;

import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.pipeline.configuration.GenotypedVcfGenericJobOptions;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.configuration.OpencgaJobOptions;

/**
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Tasklet that loads transformed variants into mongoDB
 *
 * Input: transformed variants file (variants.json.gz)
 * Output: variants loaded into mongodb
 */
@Component
@Import({GenotypedVcfGenericJobOptions.class, OpencgaJobOptions.class})
public class VariantLoaderStep implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(VariantLoaderStep.class);

    @Autowired
    private OpencgaJobOptions opencgaJobOptions;
    
    @Autowired
    private GenotypedVcfGenericJobOptions vcfJobOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Path inputFilePath = vcfJobOptions.getFilePath();
        Path outputDirectoryPath = vcfJobOptions.getOutputDirectory();

        Path outputVariantJsonFile = outputDirectoryPath.resolve(inputFilePath.getFileName().toString() + ".variants.json" + JobOptions.compressExtension);
        URI transformedVariantsUri = outputDirectoryPath.toUri().resolve(outputVariantJsonFile.getFileName().toString());

        logger.info("Loading variants from file {}", inputFilePath.toUri());

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        variantStorageManager.load(transformedVariantsUri, opencgaJobOptions.getOptions());

        return RepeatStatus.FINISHED;
    }

}
