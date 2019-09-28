package com.weibo.dip.portal.example.service;

import com.weibo.dip.portal.example.model.Dataset;

public interface DatasetService {

	public int create(Dataset dataset);

	public Dataset read(int id);

	public int update(Dataset dataset);

	public int delete(int id);

}
