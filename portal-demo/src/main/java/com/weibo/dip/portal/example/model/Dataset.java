package com.weibo.dip.portal.example.model;

public class Dataset {

	private int id;

	private String name;

	public Dataset() {

	}

	public Dataset(String name) {
		this.name = name;
	}

	public Dataset(int id, String name) {
		this.id = id;

		this.name = name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!(obj instanceof Dataset)) {
			return false;
		}

		Dataset dataset = (Dataset) obj;

		return this.id == dataset.id;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {
		return String.format("id: %s, name: %s", id, name);
	}

}
