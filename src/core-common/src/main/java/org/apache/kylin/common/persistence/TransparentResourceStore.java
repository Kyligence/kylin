/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.persistence;

import static org.apache.kylin.common.persistence.MetadataType.NEED_CACHED_METADATA;
import static org.apache.kylin.common.persistence.MetadataType.mergeKeyWithType;
import static org.apache.kylin.common.persistence.MetadataType.splitKeyWithType;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.JdbcMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

import lombok.Getter;
import lombok.val;

/**
 * Read and write metadata directly via the metadata store.
 */
public class TransparentResourceStore extends ResourceStore {

    @Getter
    private final List<RawResource> resources;

    @Getter
    private final Map<String, RawResource> auditLogDiffRs;
    
    private final boolean needToComputeRawDiff;

    public TransparentResourceStore(MetadataStore metadataStore, KylinConfig kylinConfig) {
        super(kylinConfig);
        this.metadataStore = metadataStore;
        this.resources = Lists.newArrayList();
        this.auditLogDiffRs = Maps.newHashMap();
        this.needToComputeRawDiff = kylinConfig.isAuditLogJsonPatchEnabled()
                && (metadataStore instanceof JdbcMetadataStore)
                && (kylinConfig.isUTEnv() || !UnitOfWork.get().isSkipAuditLog());
    }

    @Override
    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    @Override
    protected NavigableSet<String> listResourcesImpl(String folderPath, RawResourceFilter filter, boolean recursive) {
        MetadataType type = MetadataType.valueOf(folderPath);
        TreeSet<String> ret = new TreeSet<>();
        if (type == MetadataType.ALL) {
            if (recursive && !getConfig().isUTEnv()) {
                throw new IllegalArgumentException("Fetching all metadata in the transaction is not allowed.");
            } else if (recursive && getConfig().isUTEnv()) {
                return getMetadataStore().listAll();
            } else if (!recursive) {
                NEED_CACHED_METADATA.forEach(t -> ret.add(t.name()));
                return ret;
            }
        }
        List<RawResource> rawResources = getMetadataStore().get(type, filter, false, false);
        rawResources.forEach(raw -> ret.add(mergeKeyWithType(raw.getMetaKey(), type)));
        return ret;
    }

    @Override
    protected boolean existsImpl(String resPath) {
        return getResourceImpl(resPath, false) != null;
    }

    public void batchLock(MetadataType type, RawResourceFilter filter) {
        getMetadataStore().get(type, filter, true, false);
    }

    @Override
    protected RawResource getResourceImpl(String resPath, boolean needLock) {
        return getResourceImpl(resPath, needLock, true);
    }

    private RawResource getResourceImpl(String resPath, boolean needLock, boolean needContent) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        String metaKey = metaKeyAndType.getSecond();
        RawResourceFilter filter = RawResourceFilter.equalFilter(META_KEY_PROPERTIES_NAME, metaKey);
        val raw = getMetadataStore().get(type, filter, needLock, needContent);
        if (raw.size() != 1) {
            return null;
        }
        RawResource rawResource = raw.get(0);
        if (needLock && needToComputeRawDiff) {
            auditLogDiffRs.put(resPath, rawResource);
        }
        return rawResource;
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc) {
        return checkAndPutResource(resPath, byteSource, System.currentTimeMillis(), oldMvcc);
    }

    @Override
    public RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp, long oldMvcc) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        RawResource raw = RawResource.constructResource(type, byteSource);
        assert raw != null;
        raw.setMvcc(oldMvcc + 1);
        raw.setTs(timeStamp);
        raw.setMetaKey(metaKeyAndType.getSecond());
        int affect = getMetadataStore().save(type, raw);
        if (affect == 1) {
            // If the same metadata is written multiple times in the transaction, the last write metadata will be
            // retained in underlying for generating json patch
            if (needToComputeRawDiff) {
                RawResource before = auditLogDiffRs.get(resPath);
                raw.fillContentDiffFromRaw(before);
                auditLogDiffRs.put(resPath, raw);
            }
            resources.add(raw);
            return raw;
        } else {
            throw new IllegalStateException("Update metadata failed.");
        }
    }

    @Override
    protected void deleteResourceImpl(String resPath) {
        Pair<MetadataType, String> metaKeyAndType = splitKeyWithType(resPath);
        MetadataType type = metaKeyAndType.getFirst();
        String metaKey = metaKeyAndType.getSecond();
        RawResource raw = RawResource.constructResource(type, null);
        raw.setMetaKey(metaKey);
        getMetadataStore().save(type, raw);
        resources.add(new TombRawResource(raw.getMetaKey(), raw.getMetaType()));
        if (needToComputeRawDiff) {
            auditLogDiffRs.remove(resPath);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return toString() + ":" + resPath;
    }

    @Override
    public void reload() {
        throw new NotImplementedException("ThreadViewResourceStore doesn't support reload");
    }

    @Override
    public String toString() {
        return "<thread view metastore@" + System.identityHashCode(this) + ":KylinConfig@"
                + System.identityHashCode(kylinConfig.base()) + ">";
    }

}
