package com.weibo.dip.portal.example.dao;

import com.weibo.dip.portal.example.model.Dataset;

public interface DatasetDao {

	public int create(Dataset dataset);

	public Dataset read(int id);

	public int update(Dataset dataset);

	public int delete(int id);

}
