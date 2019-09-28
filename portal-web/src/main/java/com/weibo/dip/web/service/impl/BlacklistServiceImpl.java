package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.dao.BlacklistDao;
import com.weibo.dip.web.model.AnalyzeJson;
import com.weibo.dip.web.model.Searching;
import com.weibo.dip.web.model.intelligentwarning.Blacklist;
import com.weibo.dip.web.service.BlacklistService;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class BlacklistServiceImpl implements BlacklistService {

  @Autowired
  private BlacklistDao blacklistDao;

  @Override
  public Map<String, Object> create(Blacklist blacklist) {
    return this.blacklistDao.create(blacklist);
  }

  @Override
  public boolean delete(int id) {
    return this.blacklistDao.delete(id);
  }

  @Override
  public long update(Blacklist blacklist) {
    return this.blacklistDao.update(blacklist);
  }

  @Override
  public Blacklist findBlacklistById(int id) {
    return this.blacklistDao.findBlacklistById(id);
  }

  @Override
  public boolean isExist(String blackName) {
    return this.blacklistDao.isExist(blackName);
  }

  @Override
  public List<Blacklist> searching(AnalyzeJson json) {
    Searching searching = new Searching(json.getCondition(), json.getStarttime(), json.getEndtime(),
        json.getKeyword());
    return this.blacklistDao.searching(searching, json.getColumn(), json.getDir());
  }

}


