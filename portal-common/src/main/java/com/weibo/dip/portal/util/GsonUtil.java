package com.weibo.dip.portal.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class GsonUtil {

	private static final Gson GSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

	public static String toJson(Object object) throws Exception {
		return GSON.toJson(object);
	}

	public static String toJson(Object object, Type type) throws Exception {
		return GSON.toJson(object, type);
	}

	public static <T> T fromJson(String json, Class<T> classOfT) throws Exception {
		return GSON.fromJson(json, classOfT);
	}

	public static <T> T fromJson(String json, Type type) throws Exception {
		return GSON.fromJson(json, type);
	}

	public static class GsonType {

		// primitive
		public static final Type INT_TYPE = new TypeToken<Integer>() {
		}.getType();

		public static final Type DOUBLE_TYPE = new TypeToken<Double>() {
		}.getType();

		public static final Type BOOLEAN_TYPE = new TypeToken<Boolean>() {
		}.getType();

		public static final Type STRING_TYPE = new TypeToken<String>() {
		}.getType();

		// primitive array
		public static final Type INT_ARRAY_TYPE = new TypeToken<int[]>() {
		}.getType();

		public static final Type DOUBLE_ARRAY_TYPE = new TypeToken<double[]>() {
		}.getType();

		public static final Type BOOLEAN_ARRAY_TYPE = new TypeToken<boolean[]>() {
		}.getType();

		public static final Type STRING_ARRAY_TYPE = new TypeToken<String[]>() {
		}.getType();

		// primitive list
		public static final Type INT_LIST_TYPE = new TypeToken<List<Integer>>() {
		}.getType();

		public static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
		}.getType();

		public static final Type BOOLEAN_LIST_TYPE = new TypeToken<List<Boolean>>() {
		}.getType();

		public static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() {
		}.getType();

		// primitive map
		public static final Type INT_MAP_TYPE = new TypeToken<Map<String, Integer>>() {
		}.getType();

		public static final Type DOUBLE_MAP_TYPE = new TypeToken<Map<String, Double>>() {
		}.getType();

		public static final Type BOOLEAN_MAP_TYPE = new TypeToken<Map<String, Boolean>>() {
		}.getType();

		public static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {
		}.getType();

		// object list
		public static final Type OBJECT_LIST_TYPE = new TypeToken<List<Object>>() {
		}.getType();

		// object map
		public static final Type OBJECT_MAP_TYPE = new TypeToken<Map<String, Object>>() {
		}.getType();

	}

}
