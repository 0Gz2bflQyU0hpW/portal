package com.weibo.dip.streaming.bean;

/**
 * Executor.
 *
 * @author yurun
 */
public class Executor {
  private String id;
  private String hostPort;
  private boolean active;
  private int rddBlocks;
  private long memoryUsed;
  private long diskUsed;
  private int totalCores;
  private int maxTasks;
  private int activeTasks;
  private int failedTasks;
  private int completedTasks;
  private int totalTasks;
  private long totalDuration;
  private long totalGcTime;
  private long totalInputBytes;
  private long totalShuffleRead;
  private long totalShuffleWrite;
  private boolean blacklisted;
  private long maxMemory;
  private String stdout;
  private String stderr;
  private long usedOnHeapStorageMemory;
  private long usedOffHeapStorageMemory;
  private long totalOnHeapStorageMemory;
  private long totalOffHeapStorageMemory;

  public Executor() {}

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getHostPort() {
    return hostPort;
  }

  public void setHostPort(String hostPort) {
    this.hostPort = hostPort;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public int getRddBlocks() {
    return rddBlocks;
  }

  public void setRddBlocks(int rddBlocks) {
    this.rddBlocks = rddBlocks;
  }

  public long getMemoryUsed() {
    return memoryUsed;
  }

  public void setMemoryUsed(long memoryUsed) {
    this.memoryUsed = memoryUsed;
  }

  public long getDiskUsed() {
    return diskUsed;
  }

  public void setDiskUsed(long diskUsed) {
    this.diskUsed = diskUsed;
  }

  public int getTotalCores() {
    return totalCores;
  }

  public void setTotalCores(int totalCores) {
    this.totalCores = totalCores;
  }

  public int getMaxTasks() {
    return maxTasks;
  }

  public void setMaxTasks(int maxTasks) {
    this.maxTasks = maxTasks;
  }

  public int getActiveTasks() {
    return activeTasks;
  }

  public void setActiveTasks(int activeTasks) {
    this.activeTasks = activeTasks;
  }

  public int getFailedTasks() {
    return failedTasks;
  }

  public void setFailedTasks(int failedTasks) {
    this.failedTasks = failedTasks;
  }

  public int getCompletedTasks() {
    return completedTasks;
  }

  public void setCompletedTasks(int completedTasks) {
    this.completedTasks = completedTasks;
  }

  public int getTotalTasks() {
    return totalTasks;
  }

  public void setTotalTasks(int totalTasks) {
    this.totalTasks = totalTasks;
  }

  public long getTotalDuration() {
    return totalDuration;
  }

  public void setTotalDuration(long totalDuration) {
    this.totalDuration = totalDuration;
  }

  public long getTotalGcTime() {
    return totalGcTime;
  }

  public void setTotalGcTime(long totalGcTime) {
    this.totalGcTime = totalGcTime;
  }

  public long getTotalInputBytes() {
    return totalInputBytes;
  }

  public void setTotalInputBytes(long totalInputBytes) {
    this.totalInputBytes = totalInputBytes;
  }

  public long getTotalShuffleRead() {
    return totalShuffleRead;
  }

  public void setTotalShuffleRead(long totalShuffleRead) {
    this.totalShuffleRead = totalShuffleRead;
  }

  public long getTotalShuffleWrite() {
    return totalShuffleWrite;
  }

  public void setTotalShuffleWrite(long totalShuffleWrite) {
    this.totalShuffleWrite = totalShuffleWrite;
  }

  public boolean isBlacklisted() {
    return blacklisted;
  }

  public void setBlacklisted(boolean blacklisted) {
    this.blacklisted = blacklisted;
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public String getStdout() {
    return stdout;
  }

  public void setStdout(String stdout) {
    this.stdout = stdout;
  }

  public String getStderr() {
    return stderr;
  }

  public void setStderr(String stderr) {
    this.stderr = stderr;
  }

  public long getUsedOnHeapStorageMemory() {
    return usedOnHeapStorageMemory;
  }

  public void setUsedOnHeapStorageMemory(long usedOnHeapStorageMemory) {
    this.usedOnHeapStorageMemory = usedOnHeapStorageMemory;
  }

  public long getUsedOffHeapStorageMemory() {
    return usedOffHeapStorageMemory;
  }

  public void setUsedOffHeapStorageMemory(long usedOffHeapStorageMemory) {
    this.usedOffHeapStorageMemory = usedOffHeapStorageMemory;
  }

  public long getTotalOnHeapStorageMemory() {
    return totalOnHeapStorageMemory;
  }

  public void setTotalOnHeapStorageMemory(long totalOnHeapStorageMemory) {
    this.totalOnHeapStorageMemory = totalOnHeapStorageMemory;
  }

  public long getTotalOffHeapStorageMemory() {
    return totalOffHeapStorageMemory;
  }

  public void setTotalOffHeapStorageMemory(long totalOffHeapStorageMemory) {
    this.totalOffHeapStorageMemory = totalOffHeapStorageMemory;
  }

  @Override
  public String toString() {
    return "Executor{"
        + "id='"
        + id
        + '\''
        + ", hostPort='"
        + hostPort
        + '\''
        + ", active="
        + active
        + ", rddBlocks="
        + rddBlocks
        + ", memoryUsed="
        + memoryUsed
        + ", diskUsed="
        + diskUsed
        + ", totalCores="
        + totalCores
        + ", maxTasks="
        + maxTasks
        + ", activeTasks="
        + activeTasks
        + ", failedTasks="
        + failedTasks
        + ", completedTasks="
        + completedTasks
        + ", totalTasks="
        + totalTasks
        + ", totalDuration="
        + totalDuration
        + ", totalGcTime="
        + totalGcTime
        + ", totalInputBytes="
        + totalInputBytes
        + ", totalShuffleRead="
        + totalShuffleRead
        + ", totalShuffleWrite="
        + totalShuffleWrite
        + ", blacklisted="
        + blacklisted
        + ", maxMemory="
        + maxMemory
        + ", stdout='"
        + stdout
        + '\''
        + ", stderr='"
        + stderr
        + '\''
        + ", usedOnHeapStorageMemory="
        + usedOnHeapStorageMemory
        + ", usedOffHeapStorageMemory="
        + usedOffHeapStorageMemory
        + ", totalOnHeapStorageMemory="
        + totalOnHeapStorageMemory
        + ", totalOffHeapStorageMemory="
        + totalOffHeapStorageMemory
        + '}';
  }
}
