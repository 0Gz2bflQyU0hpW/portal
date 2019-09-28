package com.weibo.dip.data.platform.datacubic.watch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by yurun on 17/1/25.
 */
public class HDFSDatasetStatisticByDay {

    private static class Dataset {

        private String name;

        private long size;

        public Dataset() {

        }

        public Dataset(String name, long size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        @Override
        public String toString() {
            return name + "\t" + size;
        }
    }

    private static List<Dataset> getDatasetSize(String day) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        String rawlog = "/user/hdfs/rawlog";

        Path[] datasetDirs = FileUtil.stat2Paths(fs.listStatus(new Path(rawlog)));

        List<Dataset> datasets = new ArrayList<>();

        for (Path datasetDir : datasetDirs) {
            Path datasetDayDir = new Path(datasetDir, day);

            if (fs.exists(datasetDayDir) && fs.isDirectory(datasetDayDir)) {
                String name = datasetDir.getName();

                long size = fs.getContentSummary(datasetDayDir).getLength();

                datasets.add(new Dataset(name, size));
            }
        }

        datasets.sort(Comparator.comparingLong(dataset -> -dataset.getSize()));

        return datasets;
    }

    public static void main(String[] args) throws Exception {
        List<Dataset> datasets = getDatasetSize(args[0]);

        long sum = 0;

        for (Dataset dataset : datasets) {
            sum += dataset.getSize();

            System.out.println(dataset.getName() + "\t" + (dataset.getSize() / 1024 / 1024 / 1024));
        }

        System.out.println("sum: " + (sum / 1024 / 1024 / 1024));
    }

}
