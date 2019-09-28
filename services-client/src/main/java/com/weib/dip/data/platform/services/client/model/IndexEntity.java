package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Preconditions;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 16/12/22.
 */
public class IndexEntity implements Serializable {

    private String index;

    private String type;

    private String id;

    private Map<String, Object> terms;

    private Date timestamp;

    public IndexEntity() {

    }

    public IndexEntity(String index, String type) {
        this.index = index;
        this.type = type;
    }

    public IndexEntity(String index, String type, Map<String, Object> terms, Date timestamp) {
        this.index = index;
        this.type = type;
        this.terms = terms;
        this.timestamp = timestamp;
    }

    public IndexEntity(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public IndexEntity(String index, String type, String id, Map<String, Object> terms, Date timestamp) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.terms = terms;
        this.timestamp = timestamp;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getTerms() {
        return terms;
    }

    public Object getTerm(String key) {
        if (terms == null) {
            return null;
        }

        return terms.get(key);
    }

    public void setTerms(Map<String, Object> terms) {
        this.terms = terms;
    }

    public void setTerm(String key, Object value) {
        if (terms == null) {
            terms = new HashedMap();
        }

        terms.put(key, value);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public void checkState() {
        Preconditions.checkState(StringUtils.isNotEmpty(index), "index can't be empty");
        Preconditions.checkState(StringUtils.isNotEmpty(type), "type can't be empty");
        Preconditions.checkState(MapUtils.isNotEmpty(terms), "terms can't be empty");
        Preconditions.checkState(Objects.nonNull(timestamp), "timestamp can't be null");
    }

    @Override
    public String toString() {
        return "IndexEntity{" +
            "index='" + index + '\'' +
            ", type='" + type + '\'' +
            ", id='" + id + '\'' +
            ", terms=" + terms +
            ", timestamp=" + timestamp +
            '}';
    }

}
