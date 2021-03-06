package com.weibo.dip.scheduler.service;

import com.weibo.dip.scheduler.bean.Application;
import com.weibo.dip.scheduler.bean.ApplicationDependency;
import com.weibo.dip.scheduler.bean.ApplicationRecord;
import com.weibo.dip.scheduler.bean.ScheduleApplication;
import com.weibo.dip.scheduler.queue.Message;
import java.util.Date;
import java.util.List;

/**
 * Schedler Service.
 *
 * @author yurun
 */
public interface SchedulerService {
  boolean connect();

  void start(Application application) throws Exception;

  void update(Application application) throws Exception;

  void stop(String name, String queue) throws Exception;

  List<String> queues() throws Exception;

  Application get(String name, String queue) throws Exception;

  List<Application> list() throws Exception;

  List<Application> list(String queue) throws Exception;

  List<ApplicationRecord> listRunning(String queue) throws Exception;

  void addDependency(ApplicationDependency dependency) throws Exception;

  void removeDependency(String name, String queue, String dependName, String dependQueue)
      throws Exception;

  List<ApplicationDependency> getDependencies(String name, String queue) throws Exception;

  List<Message> queued() throws Exception;

  ApplicationRecord getRecord(String name, String queue, Date scheduleTime) throws Exception;

  List<ApplicationRecord> listRecords(String name, String queue, Date beginTime, Date endTime)
      throws Exception;

  List<ScheduleApplication> repair(String name, String queue, Date beginTime, Date endTime)
      throws Exception;

  boolean deleteQueued(int id) throws Exception;

  boolean kill(String name, String queue, Date scheduleTime) throws Exception;
}
