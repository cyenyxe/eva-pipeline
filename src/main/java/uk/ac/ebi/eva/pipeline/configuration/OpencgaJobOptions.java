package uk.ac.ebi.eva.pipeline.configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@JobScope
public class OpencgaJobOptions {

    private static final Logger logger = LoggerFactory.getLogger(OpencgaJobOptions.class);

    @Value("#{jobParameters['app.opencga.path']}")
    private String opencgaAppHome;
    
    @Value("#{jobParameters['input.vcf']}")
    private String inputFilePath;
    
    @Value("#{jobParameters['input.vcf.id']}")
    private String fileId;
    
    // TODO Must be mandatory for aggregated job
    @Value("#{jobParameters['input.vcf.aggregation']}")
    private String aggregated = JobOptions.defaultAggregation;
    
    @Value("#{jobParameters['input.study.type']}")
    private String studyType = JobOptions.defaultStudyType;
    
    @Value("#{jobParameters['input.study.name']}")
    private String studyName;
    
    @Value("#{jobParameters['input.study.id']}")
    private String studyId;
    
    @Value("#{jobParameters['config.db.hosts']}")
    private String dbHosts;
    
    @Value("#{jobParameters['config.db.authentication-db']}")
    private String dbAuthenticationDb;
    
    @Value("#{jobParameters['config.db.user']}")
    private String dbUser;
    
    @Value("#{jobParameters['config.db.password']}")
    private String dbPassword;
    
    @Value("#{jobParameters['db.name']}")
    private String dbName;
    
    @Value("#{jobParameters['statistics.overwrite']}")
    private Boolean overwriteStats = false;

    
    private ObjectMap options;
    
    
    public OpencgaJobOptions() {
        if (opencgaAppHome == null || opencgaAppHome.isEmpty()) {
            opencgaAppHome = System.getenv("OPENCGA_HOME") != null ? System.getenv("OPENCGA_HOME") : "/opt/opencga";
        }
        Config.setOpenCGAHome(opencgaAppHome);
	options = new ObjectMap();
    }
    
    @PostConstruct
    public void loadArgs() throws IOException {
        loadDbConnectionOptions();
        loadOpencgaOptions();
    }

    private void loadDbConnectionOptions() throws IOException {
        URI configUri = URI.create(Config.getOpenCGAHome() + "/").resolve("conf/").resolve("storage-mongodb.properties");
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));
        
        if (dbHosts == null) {
            dbHosts = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.HOSTS");
        }
        if (dbAuthenticationDb == null) {
            dbAuthenticationDb = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION.DB", "");
        }
        if (dbUser == null) {
            dbUser = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.USER", "");
        }
        if (dbPassword == null) {
            dbPassword = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.PASS", "");
        }
        if (dbName == null) {
            dbName = properties.getProperty("OPENCGA.STORAGE.MONGODB.VARIANT.DB.NAME");
        }
        
        if (dbHosts == null || dbHosts.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database hostname");
        }
        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Please provide a database name");
        }
    }

    private void loadOpencgaOptions() {
        VariantSource source = new VariantSource(
                Paths.get(inputFilePath).getFileName().toString(),
                fileId,
                studyId,
                studyName,
                VariantStudy.StudyType.valueOf(studyType),
                VariantSource.Aggregation.valueOf(aggregated));

        options.put(VariantStorageManager.VARIANT_SOURCE, source);
        options.put(VariantStorageManager.OVERWRITE_STATS, overwriteStats);
        options.put(VariantStorageManager.INCLUDE_SRC, JobOptions.includeSourceLine);
        options.put("compressExtension", JobOptions.compressExtension);
        options.put(VariantStorageManager.ANNOTATE, JobOptions.annotate);
        
        options.put(VariantStorageManager.DB_NAME, dbName);
        options.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_NAME, dbName);
        options.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_HOSTS, dbHosts);
        options.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_AUTHENTICATION_DB, dbAuthenticationDb);
        options.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_USER, dbUser);
        options.put(MongoDBVariantStorageManager.OPENCGA_STORAGE_MONGODB_VARIANT_DB_PASS, dbPassword);

        logger.debug("Using as input: {}", inputFilePath);
        logger.debug("Using as variantOptions: {}", options.entrySet().toString());
    }

    public ObjectMap getOptions() {
        return options;
    }

}
