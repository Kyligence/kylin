package org.apache.kylin.metadata.table;

import java.io.Serializable;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalTablePartitionDetail extends RootPersistentEntity implements Serializable {

    @JsonProperty("storage_path")
    private String storagePath;

    @JsonProperty("size_in_bytes")
    private long sizeInBytes;

    @JsonProperty("file_count")
    private long fileCount;

    @JsonProperty("partition_value")
    private String partitionValue;
}
