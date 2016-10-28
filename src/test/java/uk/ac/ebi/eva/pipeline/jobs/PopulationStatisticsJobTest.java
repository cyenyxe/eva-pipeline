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
package uk.ac.ebi.eva.pipeline.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.VariantStorageManager.VARIANT_SOURCE;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
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
import uk.ac.ebi.eva.test.utils.JobTestUtils;

/**
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 *
 * Test for {@link PopulationStatisticsJob}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsJobTest {

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
    public void fullPopulationStatisticsJob() throws Exception {
	final String inputFile = "/small20.vcf.gz";
	final String fileId = "1";
	final String studyId = "1";
        String dump = PopulationStatisticsJobTest.class.getResource("/dump/VariantStatsConfigurationTest_vl").getFile();
        JobTestUtils.restoreMongoDbFromDump(dump, dbName);
        
        VariantSource source = new VariantSource(
        	inputFile,
        	fileId,
        	studyId,
                "studyName",
                VariantStudy.StudyType.COLLECTION,
                VariantSource.Aggregation.NONE);

        jobOptions.getVariantOptions().put(VARIANT_SOURCE, source);

        File statsFile = new File(Paths.get(outputFolder.getRoot().getAbsolutePath())
                .resolve(VariantStorageManager.buildFilename(source)) + ".variants.stats.json.gz");
        assertFalse(statsFile.exists());  // ensure the stats file doesn't exist from previous executions

        initStatsLoadStepFiles();


        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("input.vcf.id", fileId) 
                .addString("input.study.type", "COLLECTION") 
                .addString("input.study.name", this.getClass().getSimpleName())
                .addString("input.study.id", studyId)
	        .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .addString("db.name", dbName)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // The file containing statistics should exist
        assertTrue(statsFile.exists());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }

    private void initStatsLoadStepFiles() throws IOException, InterruptedException {
        // valid variants load and stats create steps already completed
        String dump = PopulationStatisticsJobTest.class.getResource("/dump/").getFile();
        restoreMongoDbFromDump(dump, dbName);

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        File variantStatsFile = new File(PopulationStatisticsJobTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFileToDirectory(variantStatsFile, outputFolder.getRoot());

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        File sourceStatsFile = new File(PopulationStatisticsJobTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFileToDirectory(sourceStatsFile, outputFolder.getRoot());

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        File vcfFile = new File(PopulationStatisticsJobTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFileToDirectory(vcfFile, outputFolder.getRoot());
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
