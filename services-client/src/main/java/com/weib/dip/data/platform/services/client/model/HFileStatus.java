package com.weib.dip.data.platform.services.client.model;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by yurun on 16/12/20.
 */
public class HFileStatus implements Serializable {

    private String path;
    private long length;
    private boolean isdir;
    private short blockReplication;
    private long blockSize;
    private long modificationTime;
    private long accessTime;
    private HFsPermission permission;
    private String owner;
    private String group;
    private HBlockLocation[] blockLocations;

    public HFileStatus() {

    }

    public HFileStatus(String path, long length, boolean isdir, short blockReplication, long blockSize,
                       long modificationTime, long accessTime, HFsPermission permission, String owner,
                       String group, HBlockLocation[] blockLocations) {
        this.path = path;
        this.length = length;
        this.isdir = isdir;
        this.blockReplication = blockReplication;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.accessTime = accessTime;
        this.permission = permission;
        this.owner = owner;
        this.group = group;
        this.blockLocations = blockLocations;
    }

    public HFileStatus(LocatedFileStatus fileStatus) {
        this.setPath(fileStatus.getPath().toString());
        this.setLength(fileStatus.getLen());
        this.setIsdir(fileStatus.isDirectory());
        this.setBlockReplication(fileStatus.getReplication());
        this.setBlockSize(fileStatus.getBlockSize());
        this.setModificationTime(fileStatus.getModificationTime());
        this.setAccessTime(fileStatus.getAccessTime());

        HFsPermission hFsPermission = new HFsPermission();

        hFsPermission.setUser(fileStatus.getPermission().getUserAction().name());
        hFsPermission.setGroup(fileStatus.getPermission().getGroupAction().name());
        hFsPermission.setOther(fileStatus.getPermission().getOtherAction().name());

        this.setPermission(hFsPermission);
        this.setOwner(fileStatus.getOwner());
        this.setGroup(fileStatus.getGroup());

        BlockLocation[] blockLocations = fileStatus.getBlockLocations();

        HBlockLocation[] hBlockLocations = new HBlockLocation[blockLocations.length];

        for (int index = 0; index < blockLocations.length; index++) {
            hBlockLocations[index] = new HBlockLocation(blockLocations[index]);
        }

        this.setBlockLocations(hBlockLocations);
    }

    public  HFileStatus(FileStatus fileStatus){
        this.setPath(fileStatus.getPath().toString());
        this.setLength(fileStatus.getLen());
        this.setIsdir(fileStatus.isDirectory());
        this.setBlockReplication(fileStatus.getReplication());
        this.setBlockSize(fileStatus.getBlockSize());
        this.setModificationTime(fileStatus.getModificationTime());
        this.setAccessTime(fileStatus.getAccessTime());

        HFsPermission hFsPermission = new HFsPermission();

        hFsPermission.setUser(fileStatus.getPermission().getUserAction().name());
        hFsPermission.setGroup(fileStatus.getPermission().getGroupAction().name());
        hFsPermission.setOther(fileStatus.getPermission().getOtherAction().name());

        this.setPermission(hFsPermission);
        this.setOwner(fileStatus.getOwner());
        this.setGroup(fileStatus.getGroup());

    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public boolean isdir() {
        return isdir;
    }

    public void setIsdir(boolean isdir) {
        this.isdir = isdir;
    }

    public short getBlockReplication() {
        return blockReplication;
    }

    public void setBlockReplication(short blockReplication) {
        this.blockReplication = blockReplication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    public long getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

    public HFsPermission getPermission() {
        return permission;
    }

    public void setPermission(HFsPermission permission) {
        this.permission = permission;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public HBlockLocation[] getBlockLocations() {
        return blockLocations;
    }

    public void setBlockLocations(HBlockLocation[] blockLocations) {
        this.blockLocations = blockLocations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HFileStatus that = (HFileStatus) o;

        return path != null ? path.equals(that.path) : that.path == null;
    }

    @Override
    public int hashCode() {
        return path != null ? path.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "HFileStatus{" +
            "path='" + path + '\'' +
            ", length=" + length +
            ", isdir=" + isdir +
            ", blockReplication=" + blockReplication +
            ", blockSize=" + blockSize +
            ", modificationTime=" + modificationTime +
            ", accessTime=" + accessTime +
            ", permission=" + permission +
            ", owner='" + owner + '\'' +
            ", group='" + group + '\'' +
            ", blockLocations=" + Arrays.toString(blockLocations) +
            '}';
    }

    public String getName(){
        return this.path.substring(this.path.lastIndexOf('/')+1,this.path.length());
    }

}
