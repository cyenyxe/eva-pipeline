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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.makeGzipFile;
import static uk.ac.ebi.eva.test.utils.JobTestUtils.restoreMongoDbFromDump;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.opencga.storage.mongodb.variant.DBObjectToVariantAnnotationConverter;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import net.jcip.annotations.NotThreadSafe;
import uk.ac.ebi.eva.pipeline.configuration.AnnotationConfiguration;
import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.jobs.AnnotationJob;
import uk.ac.ebi.eva.test.data.VepOutputContent;
import uk.ac.ebi.eva.test.utils.JobTestUtils;


/**
 * @author Diego Poggioli
 *
 * Test for {@link AnnotationLoaderStep}. In the context it is loaded {@link AnnotationJob}
 * because {@link JobLauncherTestUtils} require one {@link org.springframework.batch.core.Job} to be present in order
 * to run properly.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { AnnotationJob.class, AnnotationConfiguration.class, JobLauncherTestUtils.class})
@NotThreadSafe
public class AnnotationLoaderStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private JobParameters jobParameters;

    private String dbName;
    private MongoClient mongoClient;

    @Before
    public void setUp() throws Exception {
        jobParameters = new JobParametersBuilder()
                .addString("db.name", this.getClass().getSimpleName())
                .addString("db.collections.variants.name", "variants")
                .addString("config.db.hosts", "localhost:27017")
                .addString("config.db.authentication-db", "")
                .addString("config.db.user", "")
                .addString("config.db.password", "")
                .addString("config.db.read-preference", "primary")
                .addString("vep.output", "/tmp/7_5_vep_annotation.tsv.gz")
                .toJobParameters();
        dbName = jobParameters.getString("db.name");
        mongoClient = new MongoClient();
    }

    @Test
    public void shouldLoadAllAnnotations() throws Exception {
        DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();

        String dump = AnnotationLoaderStepTest.class.getResource("/dump/VariantStatsConfigurationTest_vl/").getFile();
        restoreMongoDbFromDump(dump, dbName);

        makeGzipFile(VepOutputContent.vepOutputContent, jobParameters.getString("vep.output"));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(AnnotationLoaderStep.LOAD_VEP_ANNOTATION, jobParameters);

        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //check that documents have the annotation
        DBCursor cursor = collection(jobParameters.getString("db.name"), jobParameters.getString("db.collections.variants.name")).find();

        int cnt=0;
        int consequenceTypeCount = 0;
        while (cursor.hasNext()) {
            cnt++;
            DBObject dbObject = (DBObject)cursor.next().get("annot");
            if(dbObject != null){
                VariantAnnotation annot = converter.convertToDataModelType(dbObject);
                Assert.assertNotNull(annot.getConsequenceTypes());
                consequenceTypeCount += annot.getConsequenceTypes().size();
            }
        }

        assertEquals(300, cnt);
        assertTrue("Annotations not found", consequenceTypeCount>0);
    }

    /**
     * Release resources and delete the temporary output file
     */
    @After
    public void tearDown() throws Exception {
        JobTestUtils.cleanDBs(dbName);
        mongoClient.close();
    }

    private DBCollection collection(String databaseName, String collectionName) {
        return mongoClient.getDB(databaseName).getCollection(collectionName);
    }

}
