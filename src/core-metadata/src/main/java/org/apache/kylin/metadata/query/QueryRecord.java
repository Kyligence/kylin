package org.apache.kylin.metadata.query;

import java.util.List;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.model.Query;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("serial")
public class QueryRecord extends RootPersistentEntity {

    @JsonProperty
    private List<Query> queries = Lists.newArrayList();

    @JsonProperty
    private String username;

    public QueryRecord(String project, String username) {
        this.project = project;
        this.username = username;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.QUERY_RECORD;
    }

    @Override
    public String resourceName() {
        return generateResourceName(project, username);
    }

    public static String generateResourceName(String project, String username) {
        return project + "." + username;
    }
}
