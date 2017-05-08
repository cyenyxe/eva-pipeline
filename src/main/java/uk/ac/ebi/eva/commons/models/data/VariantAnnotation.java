/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.commons.models.data;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Annotations of the genomic variation
 */
@Document
public class VariantAnnotation {

    public static final String VEP_VERSION_FIELD = "vepVer";

    public static final String VEP_CACHE_VERSION_FIELD = "cacheVer";

    public static final String SIFT_FIELD = "sift";

    public static final String POLYPHEN_FIELD = "polyphen";

    public static final String SO_ACCESSION_FIELD = "so";

    public static final String XREFS_FIELD = "xrefs";

    @Field(value = VEP_VERSION_FIELD)
    private String vepVersion;

    @Field(value = VEP_CACHE_VERSION_FIELD)
    private String vepCacheVersion;

    @Field(value = SIFT_FIELD)
    private Set<Double> sifts = new HashSet<>();

    @Field(value = POLYPHEN_FIELD)
    private Set<Double> polyphens = new HashSet<>();

    @Field(value = SO_ACCESSION_FIELD)
    private Set<Integer> soAccessions = new HashSet<>();

    @Field(value = XREFS_FIELD)
    private Set<String> xrefIds = new HashSet<>();

    /**
     * Variant annotation constructor. Requires non empty values, otherwise throws {@link IllegalArgumentException}
     *
     * @param vepVersion non empty value required, otherwise throws {@link IllegalArgumentException}
     * @param vepCacheVersion non empty value required, otherwise throws {@link IllegalArgumentException}
     */
    public VariantAnnotation(String vepVersion, String vepCacheVersion) {
        Assert.hasText(vepVersion, "A non empty vepVersion is required");
        Assert.hasText(vepCacheVersion, "A non empty vepCacheVersion is required");
        this.vepVersion = vepVersion;
        this.vepCacheVersion = vepCacheVersion;
    }

    public void addSift(Double sift) {
        this.sifts.add(sift);
    }

    public void addSifts(Collection<Double> sifts) {
        this.sifts.addAll(sifts);
    }

    public void addPolyphen(Double polyphen) {
        this.polyphens.add(polyphen);
    }

    public void addPolyphens(Collection<Double> polyphens) {
        this.polyphens.addAll(polyphens);
    }

    public void addXrefIds(Set<String> xrefIds) {
        this.xrefIds.addAll(xrefIds);
    }

    public void addsoAccessions(Set<Integer> soAccessions) {
        this.soAccessions.addAll(soAccessions);
    }

    public Set<Double> getSifts() {
        return sifts;
    }

    public Set<Double> getPolyphens() {
        return polyphens;
    }

    public Set<Integer> getSoAccessions() {
        return soAccessions;
    }

    public Set<String> getXrefIds() {
        return xrefIds;
    }

    public String getVepVersion() {
        return vepVersion;
    }

    public String getVepCacheVersion() {
        return vepCacheVersion;
    }
}
