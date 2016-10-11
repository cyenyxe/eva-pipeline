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

import java.io.IOException;
import java.net.UnknownHostException;

import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.ReadPreference;

import uk.ac.ebi.eva.pipeline.configuration.JobOptions;
import uk.ac.ebi.eva.pipeline.io.readers.AnnotationFlatFileReader;
import uk.ac.ebi.eva.pipeline.io.writers.VepAnnotationMongoWriter;
import uk.ac.ebi.eva.pipeline.listeners.SkippedItemListener;
import uk.ac.ebi.eva.utils.ConnectionHelper;

/**
 * @author Diego Poggioli
 *
 * This step loads annotations into MongoDB.
 *
 * input: file written by VEP listing annotated variants
 * output: write the annotations into a given variant MongoDB collection.
 *
 *  Example file content:
 *  20_60343_G/A	20:60343	A	-	-	-	intergenic_variant	-	-	-	-	-	-
 *  20_60419_A/G	20:60419	G	-	-	-	intergenic_variant	-	-	-	-	-	-
 *  20_60479_C/T	20:60479	T	-	-	-	intergenic_variant	-	-	-	-	-	rs149529999	GMAF=T:0.0018;AFR_MAF=T:0.01;AMR_MAF=T:0.0028
 *
 * each line of the file is loaded with {@link AnnotationFlatFileReader} into a {@link VariantAnnotation} and then sent
 * to mongo with {@link VepAnnotationMongoWriter}.
 */

@Configuration
@EnableBatchProcessing
@Import({JobOptions.class})
public class AnnotationLoaderStep {

    public static final String LOAD_VEP_ANNOTATION = "Load VEP annotation";

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobOptions jobOptions;

    @Autowired
    private MongoOperations mongoOperations;

    @Autowired
    private ItemWriter writer;

    @Bean
    @Qualifier("annotationLoad")
    public Step annotationLoadBatchStep() throws IOException {
        return stepBuilderFactory.get(LOAD_VEP_ANNOTATION).<VariantAnnotation, VariantAnnotation> chunk(10)
                .reader(new AnnotationFlatFileReader(jobOptions.getPipelineOptions().getString("vep.output")))
                .writer(writer)
                .faultTolerant().skipLimit(50).skip(FlatFileParseException.class)
                .listener(new SkippedItemListener())
                .build();
    }

    @Bean
    @StepScope
    public VepAnnotationMongoWriter getWriter(@Value("#{jobParameters['db.collections.variants.name']}") String collection) {
        return new VepAnnotationMongoWriter(mongoOperations, collection);
    }

    @Bean
    @JobScope
    public MongoOperations getMongoOperationsFromPipelineOptions(
            @Value("#{jobParameters['db.name']}") String dbName,
            @Value("#{jobParameters['config.db.hosts']}") String hosts,
            @Value("#{jobParameters['config.db.authentication-db']}") String authenticationDb,
            @Value("#{jobParameters['config.db.user']}") String user,
            @Value("#{jobParameters['config.db.password']}") String password,
            @Value("#{jobParameters['config.db.read-preference']}") String readPreference) {
        MongoTemplate mongoTemplate;
        try {
            mongoTemplate = getMongoTemplate(hosts, dbName, authenticationDb, user, password, readPreference);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to initialize mongo template", e);
        }
        return mongoTemplate;
    }

    private static MongoTemplate getMongoTemplate(String hosts, String dbName, String authenticationDb,
            String user, String password, String readPreference) throws UnknownHostException {
        MongoTemplate mongoTemplate;
        if(authenticationDb.isEmpty()) {
            mongoTemplate = ConnectionHelper.getMongoTemplate(dbName);
        } else {
            mongoTemplate = ConnectionHelper.getMongoTemplate(dbName, hosts, authenticationDb, user, password.toCharArray());
        }

        mongoTemplate.setReadPreference(getMongoTemplateReadPreferences(readPreference));
        return mongoTemplate;
	  }

    private static ReadPreference getMongoTemplateReadPreferences(String readPreference){
        switch (readPreference){
            case "primary":
                return ReadPreference.primary();
            case "secondary":
                return ReadPreference.secondary();
            default:
                throw new IllegalArgumentException(
                        String.format("%s is not a valid ReadPreference type, please use \"primary\" or \"secondary\"",
                                readPreference));
        }

    }

}
