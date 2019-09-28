package com.weibo.dip.databus.hdfs;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by yurun on 17/12/15.
 */
public class HDFSGZipBadFileClear {

    private static class Reader implements Runnable {

        private Path path;

        public Reader(Path path) {
            this.path = path;
        }

        @Override
        public void run() {
            System.out.println(path.toString());

            boolean delete = false;

            BufferedReader reader = null;

            try {
                reader = new BufferedReader(
                    new InputStreamReader(
                        HDFSUtil.openInputStreamIgnoreCompress(path),
                        CharEncoding.UTF_8));

                while (reader.readLine() != null) {

                }
            } catch (Exception e) {
                if (e instanceof EOFException) {
                    delete = true;
                }
            } finally {
                if (Objects.nonNull(reader)) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                    }
                }
            }

            if (delete) {
                try {
                    HDFSUtil.deleteFile(path);

                    System.out.println(path.toString() + " Deleted");
                } catch (IOException e) {
                    System.out.println(path.toString() + " Delete Error");
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        List<Path> paths = HDFSUtil.listFiles(args[0], true);

        ExecutorService executor = Executors.newFixedThreadPool(Integer.valueOf(args[1]));

        for (Path path : paths) {
            executor.submit(new Reader(path));
        }

        executor.shutdown();

        while (!executor.isTerminated()) {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

}
