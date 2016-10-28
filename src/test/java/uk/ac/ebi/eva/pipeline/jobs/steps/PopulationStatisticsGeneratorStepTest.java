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
package uk.ac.ebi.eva.pipeline.jobs.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import uk.ac.ebi.eva.pipeline.configuration.CommonConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.PopulationStatisticsJob;
import uk.ac.ebi.eva.pipeline.jobs.flows.PopulationStatisticsFlow;
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Diego Poggioli
 * @author Cristina Yenyxe Gonzalez Garcia
 *
 * Test for {@link PopulationStatisticsGeneratorStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsGeneratorStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    
    @Autowired
    private JobOptions jobOptions;

    private String dbName;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    @Rule
    public TestName name = new TestName();

    @Test
    public void statisticsGeneratorStepShouldCalculateStats() throws IOException, InterruptedException {
	final String inputFile = "/small20.vcf.gz";
	final String fileId = "1";
	final String studyId = "1";
        String dump = PopulationStatisticsGeneratorStepTest.class.getResource("/dump/VariantStatsConfigurationTest_vl").getFile();
        restoreMongoDbFromDump(dump, dbName);

        VariantSource source = new VariantSource(
                inputFile,
                fileId,
                studyId,
                this.getClass().getSimpleName(),
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        jobOptions.getVariantOptions().put(VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputFolder.getRoot().getAbsolutePath()).resolve(VariantStorageManager.buildFilename(source))
                + ".variants.stats.json.gz");
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        // When the execute method in variantsStatsCreate is executed
        JobParameters jobParameters = new JobParametersBuilder()
        	.addString("input.vcf", inputFile)
        	.addString("input.vcf.id", fileId)
        	.addString("input.study.type", "COLLECTION")
        	.addString("input.study.name", this.getClass().getSimpleName())
        	.addString("input.study.id", studyId)
        	.addString("db.name", dbName)
        	.toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.CALCULATE_STATISTICS, jobParameters);

        //Then variantsStatsCreate step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //and the file containing statistics should exist
        assertTrue(statsFile.exists());
    }

    /**
     * This test has to fail because it will try to extract variants from a non-existent DB.
     * Variants not loaded.. so nothing to query!
     */
    @Test
    public void statisticsGeneratorStepShouldFailIfVariantLoadStepIsNotCompleted() throws Exception {
	final String inputFile = "/small20.vcf.gz";
	final String fileId = "1";
	final String studyId = "1";

        VariantSource source = new VariantSource(
                inputFile,
                fileId,
                studyId,
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        jobOptions.getVariantOptions().put(VARIANT_SOURCE, source);

        // When the execute method in variantsStatsCreate is executed
        JobParameters jobParameters = new JobParametersBuilder()
        	.addString("input.vcf", inputFile)
        	.addString("input.vcf.id", fileId)
        	.addString("input.study.type", "COLLECTION")
        	.addString("input.study.name", this.getClass().getSimpleName())
        	.addString("input.study.id", studyId)
        	.addString("db.name", dbName)
        	.toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.CALCULATE_STATISTICS, jobParameters);
        assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
    }

    @Before
    public void setUp() throws Exception {
        jobOptions.loadArgs();
        jobOptions.getPipelineOptions().put("output.dir.statistics", outputFolder.getRoot().getAbsolutePath());
        dbName = name.getMethodName();
    }

    @After
    public void tearDown() throws Exception {
	JobTestUtils.cleanDBs(dbName);
    }

}
