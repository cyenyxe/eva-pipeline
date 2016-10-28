/*
 * Copyright 2015-2016 EMBL - European Bioinformatics Institute
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
import java.util.Map;

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
import uk.ac.ebi.eva.pipeline.configuration.OpencgaJobOptions;
import uk.ac.ebi.eva.utils.URLHelper;

/**
 *
 * @author Jose Miguel Mut Lopez
 * @author Cristina Yenyxe Gonzalez Garcia
 *
 * Tasklet that normalizes variants. To see the applied rules please refer to:
 *
 * @see <a href="www.ebi.ac.uk/eva/?FAQ">www.ebi.ac.uk/eva/?FAQ</a>
 * @see <a href="https://docs.google.com/presentation/d/1WqSiT5AEEQF9jdIewdYIp-I0G5ozkFP3IikfCJZO1dc/edit#slide=id.ge1548f905_0_592">EVA FAQ</a>
 *
 * Input: VCF file
 * Output: transformed variants JSON file (variants.json.gz)
 */
@Component
@Import({GenotypedVcfGenericJobOptions.class, OpencgaJobOptions.class})
public class VariantNormalizerStep implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(VariantNormalizerStep.class);

    @Autowired
    private OpencgaJobOptions opencgaJobOptions;
    
    @Autowired
    private GenotypedVcfGenericJobOptions vcfJobOptions;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();

        Path inputFilePath = vcfJobOptions.getFilePath();
        Path outputDirectoryPath = vcfJobOptions.getOutputDirectory();
        URI pedigreeUri = jobParameters.get("input.pedigree") != null ? URLHelper.createUri(String.valueOf(jobParameters.get("input.pedigree"))) : null;

        logger.info("Normalizing file {} into folder {}", inputFilePath.toString(), outputDirectoryPath.toString());

        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        variantStorageManager.transform(inputFilePath.toUri(), pedigreeUri, outputDirectoryPath.toUri(), opencgaJobOptions.getOptions());
        return RepeatStatus.FINISHED;
    }

}
