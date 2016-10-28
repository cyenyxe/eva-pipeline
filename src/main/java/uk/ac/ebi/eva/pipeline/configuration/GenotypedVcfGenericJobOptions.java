package uk.ac.ebi.eva.pipeline.configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@JobScope
public class GenotypedVcfGenericJobOptions {
    
    @Value("#{jobParameters['input.vcf']}")
    private String inputFilePath;
    
    @Value("#{jobParameters['input.vcf.id']}")
    private String fileId;
    
    // TODO Must be mandatory for aggregated job
    @Value("#{jobParameters['input.vcf.aggregation']}")
    private String aggregated = "NONE";
    
    @Value("#{jobParameters['input.study.type']}")
    private String studyType;
    
    @Value("#{jobParameters['input.study.name']}")
    private String studyName;
    
    @Value("#{jobParameters['input.study.id']}")
    private String studyId;

    @Value("#{jobParameters['output.dir']}")
    private String outputDirectory;


    public Path getFilePath() {
        return Paths.get(inputFilePath);
    }

    public String getFileId() {
        return fileId;
    }

    public String getAggregated() {
        return aggregated;
    }

    public String getStudyType() {
        return studyType;
    }

    public String getStudyName() {
        return studyName;
    }

    public String getStudyId() {
        return studyId;
    }

    public Path getOutputDirectory() {
        return Paths.get(outputDirectory);
    }

}
