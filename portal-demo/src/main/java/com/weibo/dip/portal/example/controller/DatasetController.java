package com.weibo.dip.portal.example.controller;

import com.weibo.dip.portal.example.model.Dataset;
import com.weibo.dip.portal.example.service.DatasetService;
import com.weibo.dip.portal.util.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dataset")
public class DatasetController {

	@Autowired
	private DatasetService datasetService;

	@RequestMapping("create")
	public HttpResponse create(String name) {
		return new HttpResponse(HttpStatus.OK, datasetService.create(new Dataset(name)));
	}

	@RequestMapping("read")
	public HttpResponse read(@RequestParam(value = "id", required = false, defaultValue = "1") int id) {
		return new HttpResponse(HttpStatus.OK, datasetService.read(id));
	}

	@RequestMapping("update")
	public HttpResponse update(int id, String name) {
		return new HttpResponse(HttpStatus.OK, datasetService.update(new Dataset(id, name)));
	}

	@RequestMapping("delete")
	public HttpResponse delete(int id) {
		return new HttpResponse(HttpStatus.OK, datasetService.delete(id));
	}

}
