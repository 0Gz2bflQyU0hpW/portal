package com.weib.dip.data.platform.services.client.model;

import org.apache.hadoop.fs.BlockLocation;

import java.io.Serializable;

/**
 * Created by yurun on 16/12/20.
 */
public class HBlockLocation extends BlockLocation implements Serializable {

    public HBlockLocation() {
    }

    public HBlockLocation(BlockLocation that) {
        super(that);
    }

    public HBlockLocation(String[] names, String[] hosts, long offset, long length) {
        super(names, hosts, offset, length);
    }

    public HBlockLocation(String[] names, String[] hosts, long offset, long length, boolean corrupt) {
        super(names, hosts, offset, length, corrupt);
    }

    public HBlockLocation(String[] names, String[] hosts, String[] topologyPaths, long offset, long length) {
        super(names, hosts, topologyPaths, offset, length);
    }

    public HBlockLocation(String[] names, String[] hosts, String[] topologyPaths, long offset, long length, boolean corrupt) {
        super(names, hosts, topologyPaths, offset, length, corrupt);
    }

    public HBlockLocation(String[] names, String[] hosts, String[] cachedHosts, String[] topologyPaths, long offset, long length, boolean corrupt) {
        super(names, hosts, cachedHosts, topologyPaths, offset, length, corrupt);
    }

}
