package uk.ac.ebi.eva.pipeline.jobs.steps;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
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
 *
 * Test for {@link PopulationStatisticsLoaderStep}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JobOptions.class, PopulationStatisticsJob.class, CommonConfiguration.class, JobLauncherTestUtils.class})
public class PopulationStatisticsLoaderStepTest {

    private static final String SMALL_VCF_FILE = "/small20.vcf.gz";

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
    public void statisticsLoaderStepShouldLoadStatsIntoDb() throws StorageManagerException, IllegalAccessException,
            ClassNotFoundException, InstantiationException, IOException, InterruptedException {
        //Given a valid VCF input file
        String inputFile = PopulationStatisticsLoaderStepTest.class.getResource(SMALL_VCF_FILE).getFile();

        //and a valid variants load and stats create steps already completed
        String dump = PopulationStatisticsLoaderStepTest.class.getResource("/dump/VariantStatsConfigurationTest_vl").getFile();
        restoreMongoDbFromDump(dump, dbName);

        // copy stat file to load
        String variantsFileName = "/1_1.variants.stats.json.gz";
        File variantStatsFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(variantsFileName).getFile());
        FileUtils.copyFileToDirectory(variantStatsFile, outputFolder.getRoot());

        // copy source file to load
        String sourceFileName = "/1_1.source.stats.json.gz";
        File sourceStatsFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(sourceFileName).getFile());
        FileUtils.copyFileToDirectory(sourceStatsFile, outputFolder.getRoot());

        // copy transformed vcf
        String vcfFileName = "/small20.vcf.gz.variants.json.gz";
        File vcfFile = new File(PopulationStatisticsLoaderStepTest.class.getResource(vcfFileName).getFile());
        FileUtils.copyFileToDirectory(vcfFile, outputFolder.getRoot());

        // When the execute method in variantsStatsLoad is executed
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("input.vcf.id", "1") 
                .addString("input.study.type", "COLLECTION") 
                .addString("input.study.name", this.getClass().getSimpleName())
                .addString("input.study.id", "1")
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .addString("db.name", dbName)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS, jobParameters);

        // Then variantsStatsLoad step should complete correctly
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // The DB docs should have the field "st"
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(dbName, null);
        VariantDBIterator iterator = variantDBAdaptor.iterator(new QueryOptions());
        assertEquals(1, iterator.next().getSourceEntries().values().iterator().next().getCohortStats().size());
    }

    @Test
    public void statisticsLoaderStepShouldFaildBecauseVariantStatsFileIsMissing() throws JobExecutionException, IOException {
        String inputFile = PopulationStatisticsLoaderStepTest.class.getResource(SMALL_VCF_FILE).getFile();

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("input.vcf", inputFile)
                .addString("input.vcf.id", name.getMethodName()) 
                .addString("input.study.type", "COLLECTION") 
                .addString("input.study.name", this.getClass().getSimpleName())
                .addString("input.study.id", "1")
                .addString("output.dir", outputFolder.getRoot().getCanonicalPath())
                .addString("db.name", dbName)
                .toJobParameters();
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(PopulationStatisticsFlow.LOAD_STATISTICS, jobParameters);

        assertEquals(inputFile, jobParameters.getString("input.vcf"));
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
        JobTestUtils.cleanDBs(jobOptions.getDbName());
    }

}
