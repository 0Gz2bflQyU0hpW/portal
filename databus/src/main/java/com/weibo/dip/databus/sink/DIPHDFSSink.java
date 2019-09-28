package com.weibo.dip.databus.sink;

import com.hadoop.compression.lzo.LzopCodec;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.IPUtil;
import com.weibo.dip.data.platform.commons.util.ProcessUtil;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by yurun on 17/10/23.
 */
public class DIPHDFSSink extends Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(DIPHDFSSink.class);

    private org.apache.hadoop.conf.Configuration configuration;
    private FileSystem filesystem;

    {
        try {
            configuration = new org.apache.hadoop.conf.Configuration();
            filesystem = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final String HDFS_BASE_DIR = "/user/hdfs/rawlog";

    private static final SimpleDateFormat FILE_PATH_DATE_FORMAT = new SimpleDateFormat("yyyy_MM_dd/HH");
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

    /*
        hdfs_base_dir/dataset/day_and_hour/filename
     */
    private static final String FILE_PATH_PATTERN = "%s/%s/%s/%s";
    /*
        dataset-localip-processid-threadid-timestamp.extension
     */
    private static final String FILE_NAME_PATTERN = "%s-%s-%s-%s-%s.%s";

    private static final String DEFAULT_EXTENSION = "log";
    private static final String DEFLATE_EXTENSION = "deflate";
    private static final String GZIP_EXTENSION = "gz";
    private static final String BZIP2_EXTENSION = "bz2";
    private static final String LZO_EXTENSION = "lzo";
    private static final String LZ4_EXTENSION = "lz4";
    private static final String SNAPPY_EXTENSION = "snappy";

    private static final String COMPRESSION = "compression";

    private String compression;

    private class HDFSFileStream implements Closeable {

        private String dataset;
        private long timestamp;
        private String srcFilePath;
        private String dstFilePath;
        private Class<?> codecClass;
        private BufferedWriter writer;

        public HDFSFileStream(String dataset, long timestamp, String srcFilePath, String dstFilePath,
                              Class<?> codecClass) throws IOException {
            this.dataset = dataset;
            this.timestamp = timestamp;
            this.srcFilePath = srcFilePath;
            this.dstFilePath = dstFilePath;
            this.codecClass = codecClass;

            if (Objects.isNull(codecClass)) {
                this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                        filesystem.create(new Path(srcFilePath)), CharEncoding.UTF_8));
            } else {
                this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                        ((CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration))
                            .createOutputStream(
                                filesystem.create(new Path(srcFilePath))), CharEncoding.UTF_8));
            }

            LOGGER.info("hdfs file {} created", srcFilePath);
        }

        public String getDataset() {
            return dataset;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getSrcFilePath() {
            return srcFilePath;
        }

        public String getDstFilePath() {
            return dstFilePath;
        }

        public void write(String line) throws IOException {
            if (StringUtils.isEmpty(line)) {
                return;
            }

            writer.write(line);

            if (line.charAt(line.length() - 1) != '\n') {
                writer.newLine();
            }
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();

                filesystem.rename(new Path(srcFilePath), new Path(dstFilePath));
            }
        }

    }

    private synchronized HDFSFileStream createHDFSFileStream(String dataset) throws Exception {
        Date now = new Date(System.currentTimeMillis());

        String dayAndHour = FILE_PATH_DATE_FORMAT.format(now);

        String fileExtension = DEFAULT_EXTENSION;
        Class<?> codecClass = null;

        if (StringUtils.isNotEmpty(compression)) {
            switch (compression) {
                case DEFLATE_EXTENSION:
                    fileExtension = DEFAULT_EXTENSION;
                    codecClass = DefaultCodec.class;

                    break;

                case GZIP_EXTENSION:
                    fileExtension = GZIP_EXTENSION;
                    codecClass = GzipCodec.class;

                    break;

                case BZIP2_EXTENSION:
                    fileExtension = BZIP2_EXTENSION;
                    codecClass = BZip2Codec.class;

                    break;

                case LZO_EXTENSION:
                    fileExtension = LZO_EXTENSION;
                    codecClass = LzopCodec.class;

                    break;

                case LZ4_EXTENSION:
                    fileExtension = LZ4_EXTENSION;
                    codecClass = Lz4Codec.class;

                    break;

                case SNAPPY_EXTENSION:
                    fileExtension = SNAPPY_EXTENSION;
                    codecClass = SnappyCodec.class;

                    break;
            }
        }

        String fileName = String.format(FILE_NAME_PATTERN,
            dataset, IPUtil.getLocalhost(), ProcessUtil.getPid(),
            Thread.currentThread().getId(), FILE_NAME_DATE_FORMAT.format(now), fileExtension);
        String hideFileName = Symbols.FULL_STOP + fileName;

        String srcFilePath = String.format(FILE_PATH_PATTERN, HDFS_BASE_DIR,
            dataset, dayAndHour, hideFileName);
        String dstFilePath = String.format(FILE_PATH_PATTERN, HDFS_BASE_DIR,
            dataset, dayAndHour, fileName);

        return new HDFSFileStream(dataset, now.getTime(), srcFilePath, dstFilePath, codecClass);
    }

    private Map<String, HDFSFileStream> streams = new HashMap<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private long interval = 5 * 60 * 1000;

    private Flusher flusher;

    @Override
    public void setConf(Configuration conf) throws Exception {
        name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + DIPHDFSSink.class.getSimpleName();

        compression = conf.get(COMPRESSION);
    }

    private class Flusher extends Thread {

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    LOGGER.warn("{} flusher may be running, but interrupted", name);

                    break;
                }

                lock.writeLock().lock();

                try {
                    flush(System.currentTimeMillis());
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

    }

    @Override
    public void start() {
        LOGGER.info("{} starting...", name);

        flusher = new Flusher();

        flusher.start();

        LOGGER.info("{} started", name);
    }

    private void flush(long now) {
        Iterator<Map.Entry<String, HDFSFileStream>> iterator = streams.entrySet().iterator();

        while (iterator.hasNext()) {
            HDFSFileStream stream = iterator.next().getValue();

            if (now - stream.getTimestamp() >= interval) {
                iterator.remove();

                try {
                    stream.close();

                    LOGGER.info("{} flusher close {} success: {}",
                        name, stream.getDataset(), stream.getSrcFilePath());
                } catch (IOException e) {
                    LOGGER.error("{} flusher close {} error: {}",
                        name, stream.getSrcFilePath(), ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
    }

    @Override
    public void process(Message message) throws Exception {
        String dataset = message.getTopic();
        String line = message.getData();

        lock.readLock().lock();

        if (!streams.containsKey(dataset)) {
            lock.readLock().unlock();

            lock.writeLock().lock();

            try {
                if (!streams.containsKey(dataset)) {
                    streams.put(dataset, createHDFSFileStream(dataset));
                }

                lock.readLock().lock();
            } finally {
                lock.writeLock().unlock();
            }
        }

        try {
            streams.get(dataset).write(line);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void stop() {
        LOGGER.info("{} stoping...", name);

        flusher.interrupt();

        try {
            flusher.join();
        } catch (InterruptedException e) {
            LOGGER.warn("{} flusher may be waitting for stop, but interrupted");
        }

        flush(Long.MAX_VALUE);

        LOGGER.info("{} stoped", name);
    }

}
