package com.weibo.dip.portal.example.service.impl;

import com.weibo.dip.portal.example.dao.DatasetDao;
import com.weibo.dip.portal.example.model.Dataset;
import com.weibo.dip.portal.example.service.DatasetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service("datasetService")
@Transactional
public class DatasetServiceImpl implements DatasetService {

	@Autowired
	private DatasetDao datasetDao;

	@Override
	public int create(Dataset dataset) {
		return datasetDao.create(dataset);
	}

	@Override
	public Dataset read(int id) {
		return datasetDao.read(id);
	}

	@Override
	public int update(Dataset dataset) {
		return datasetDao.update(dataset);
	}

	@Override
	public int delete(int id) {
		return datasetDao.delete(id);
	}

}
