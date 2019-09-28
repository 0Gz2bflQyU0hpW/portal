package com.weib.dip.data.platform.services.client.model;

import org.apache.hadoop.fs.ContentSummary;

import java.io.Serializable;

/**
 * Created by yurun on 16/12/20.
 */
public class HContentSummary extends ContentSummary implements Serializable {

    public HContentSummary() {
    }

    public HContentSummary(long length, long fileCount, long directoryCount) {
        super(length, fileCount, directoryCount);
    }

    public HContentSummary(long length, long fileCount, long directoryCount, long quota, long spaceConsumed, long spaceQuota) {
        super(length, fileCount, directoryCount, quota, spaceConsumed, spaceQuota);
    }

    public HContentSummary(ContentSummary contentSummary) {
        super(contentSummary.getLength(), contentSummary.getFileCount(), contentSummary.getDirectoryCount(),
            contentSummary.getQuota(), contentSummary.getSpaceConsumed(), contentSummary.getSpaceQuota());
    }

}
