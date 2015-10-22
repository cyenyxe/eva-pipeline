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
package embl.ebi.variation.eva.pipeline.jobs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Created by jmmut on 2015-10-14.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {VariantConfiguration.class})
public class VariantConfigurationTest {

    public static final String FILE_20 = "/small20.vcf.gz";
    public static final String FILE_22 = "/small22.vcf.gz";
    public static final String FILE_WITH_ERROR = "/wrong.vcf.gz";

    private static final Logger logger = LoggerFactory.getLogger(VariantConfigurationTest.class);

    @Autowired
    VariantConfiguration variantConfiguration;

    @Autowired
    private Job job;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobExplorer jobExplorer;

    @Test
    public void validTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_20).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        
        JobParameters parameters = new JobParametersBuilder()
                .addString("input", input)
                .addString("outputDir", "/tmp")
                .addString("dbName", "validTransformTest")
                .addString("compressExtension", ".gz")
                .addString("compressGenotypes", "true")
                .addString("includeSrc", "FIRST_8_COLUMNS")
                .addString("aggregated", "NONE")
                .addString("studyType", "COLLECTION")
                .addString("studyName", "studyName")
                .addString("studyId", "1")
                .addString("fileId", "1")
                .addString("opencga.app.home", opencgaHome)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, parameters);

        assertEquals(input, execution.getJobParameters().getString("input"));
        assertEquals("COMPLETED", execution.getExitStatus().getExitCode());

        ////////// check transformed file
        String outputFilename = getTransformedOutputPath(Paths.get(FILE_20).getFileName(), 
                parameters.getString("compressExtension"), parameters.getString("outputDir"));
        logger.info("reading transformed output from: " + outputFilename);


        BufferedReader file = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(outputFilename))));
        long lines = 0;
        while (file.readLine() != null) {
            lines++;
        }
        file.close();
        assertEquals(300, lines);
    }

    @Test
    public void invalidTransform() throws Exception {
        String input = VariantConfigurationTest.class.getResource(FILE_WITH_ERROR).getFile();
        String opencgaHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";

        JobParameters parameters = new JobParametersBuilder()
                .addString("input", input)
                .addString("outputDir", "/tmp")
                .addString("dbName", "invalidTransformTest")
                .addString("compressExtension", ".gz")
                .addString("compressGenotypes", "true")
                .addString("includeSrc", "FIRST_8_COLUMNS")
                .addString("aggregated", "NONE")
                .addString("studyType", "COLLECTION")
                .addString("studyName", "studyName")
                .addString("studyId", "2")
                .addString("fileId", "2")
                .addString("opencga.app.home", opencgaHome)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, parameters);

        assertEquals(input, execution.getJobParameters().getString("input"));
        assertEquals("FAILED", execution.getExitStatus().getExitCode());
    }

    @Test
    public void validLoad() {

    }

    @Test
    public void invalidLoad() {

    }

    @Test
    public void validCreateStats() {

    }

    @Test
    public void invalidCreateStats() {

    }
    @Test
    public void validLoadStats() {

    }

    @Test
    public void invalidLoadStats() {

    }
    
    private String getTransformedOutputPath(Path input, String compressExtension, String outputDir) {
        return Paths.get(outputDir).resolve(input) + ".variants.json" + compressExtension;
    }

}