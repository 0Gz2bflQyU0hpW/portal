package com.weibo.dip.iplibrary;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

/**
 * Ip library.
 *
 * @author yurun
 */
public class IpLibraryV6 implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IpLibraryV6.class);

  public static final String DEFAULT_IP_LIBRARY_LOCATION =
      "/data0/dipplat/software/systemfile/iplibraryv6";

  private static final long DEFAULT_UPDATE_INTERVAL = 10 * 60 * 1000;

  private String libraryDir;
  private String library;
  private boolean useRam;
  private boolean useCache;

  private long interval;

  private ReadWriteLock libraryReadWriteLock = new ReentrantReadWriteLock();
  private Lock libraryReadLock = libraryReadWriteLock.readLock();
  private Lock libraryWriteLock = libraryReadWriteLock.writeLock();

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  private String[] ippoints;
  private Map<String, Location> cache = null;
  private IndexSearcher indexSearcher;

  private void release() throws IOException {
    final Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    if (useCache) {
      if (Objects.nonNull(cache)) {
        cache.clear();
        cache = null;
      }
    }

    ippoints = null;

    if (Objects.nonNull(indexSearcher)) {
      indexSearcher.getIndexReader().close();

      indexSearcher = null;
    }

    stopwatch.stop();

    LOGGER.info("library release time: {} ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
  }

  private void reinitialize() throws Exception {
    Preconditions.checkState(StringUtils.isNotEmpty(libraryDir), "libraryDir is empty");

    File libraryLocation = new File(libraryDir);
    Preconditions.checkState(libraryLocation.exists(), "libraryDir %s does not exist", libraryDir);

    // list all libraries
    String[] libraries = libraryLocation.list();
    Preconditions.checkState(
        ArrayUtils.isNotEmpty(libraries), "libraryDir %s does not have any libraries", libraryDir);

    // find the latest library
    Arrays.sort(libraries);

    File lastestLibrary = new File(libraryLocation, libraries[libraries.length - 1]);
    Preconditions.checkState(
        lastestLibrary.isDirectory(),
        "latest library %s is not a directory",
        lastestLibrary.getName());

    libraryReadLock.lock();

    try {
      // if the library is already the latest, return
      if (StringUtils.isNotEmpty(library) && lastestLibrary.getName().equals(library)) {
        LOGGER.info("library {} is latest, no update required", library);

        return;
      }
    } finally {
      libraryReadLock.unlock();
    }

    // library is empty, or need to be updated
    libraryWriteLock.lock();

    try {
      release();

      final Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();

      library = lastestLibrary.getName();
      LOGGER.info("library: {}", library);

      /*
       * initialize cache
       */
      if (useCache) {
        cache = new HashMap<>();
        LOGGER.info("initialize cache success");
      }

      /*
       * initialize ippoints
       */
      File ippointsFile = new File(lastestLibrary, "ippoints");
      Preconditions.checkState(ippointsFile.exists(), "library dose not include ippoints(file)");

      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(new FileInputStream(ippointsFile), CharEncoding.UTF_8))) {

        String line = reader.readLine();

        ippoints = new String[Integer.valueOf(line)];
        LOGGER.info("ip points: {}", ippoints.length);

        int index = 0;
        while ((line = reader.readLine()) != null) {
          if (StringUtils.isEmpty(line)) {
            continue;
          }

          ippoints[index++] = line;
        }

        LOGGER.info("initialize ippoints success");
      }

      /*
       * initialize lucene index
       */
      File indexDir = new File(lastestLibrary, "index");
      Preconditions.checkState(indexDir.exists(), "library dose not include index(dir)");

      Directory luceneIndexDir = FSDirectory.open(indexDir);

      IndexReader indexReader;

      if (useRam) {
        indexReader = DirectoryReader.open(new RAMDirectory(luceneIndexDir, new IOContext()));
      } else {
        indexReader = DirectoryReader.open(luceneIndexDir);
      }

      indexSearcher = new IndexSearcher(indexReader);
      LOGGER.info("initialize lucene index search success");

      stopwatch.stop();

      LOGGER.info("library initialize time: {} ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
    } finally {
      libraryWriteLock.unlock();
    }
  }

  private class DynamicLoader implements Runnable {

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        try {
          reinitialize();
        } catch (Exception e) {
          LOGGER.error("dynamic load library error: {}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
  }

  public IpLibraryV6() throws Exception {
    this(DEFAULT_IP_LIBRARY_LOCATION);
  }

  public IpLibraryV6(boolean useCache) throws Exception {
    this(DEFAULT_IP_LIBRARY_LOCATION, false, useCache);
  }

  public IpLibraryV6(String libraryDir) throws Exception {
    this(libraryDir, false, false, DEFAULT_UPDATE_INTERVAL);
  }

  public IpLibraryV6(String libraryDir, boolean useCache) throws Exception {
    this(libraryDir, false, useCache, DEFAULT_UPDATE_INTERVAL);
  }

  public IpLibraryV6(String libraryDir, boolean useRam, boolean useCache) throws Exception {
    this(libraryDir, useRam, useCache, DEFAULT_UPDATE_INTERVAL);
  }

  /**
   * Construct a IpLibrary instance.
   *
   * @param libraryDir ip library dir in disk
   * @param useRam whether to use ram
   * @param useCache whether to use cache
   * @param interval ip library index load interval
   * @throws Exception if initialize ip library error
   */
  public IpLibraryV6(String libraryDir, boolean useRam, boolean useCache, long interval)
      throws Exception {
    this.libraryDir = libraryDir;
    this.useRam = useRam;
    this.useCache = useCache;
    this.interval = interval;

    reinitialize();

    executor.submit(new DynamicLoader());
  }

  private Location toLocation(String ip, String ipPoint) {
    Document document;

    try {
      Query query = new TermQuery(new Term("1", ipPoint));

      TopDocs docs = indexSearcher.search(query, 1);

      if (docs.totalHits < 1) {
        return null;
      }

      document = indexSearcher.doc(docs.scoreDocs[0].doc);
    } catch (IOException e) {
      LOGGER.debug("search index error: {}", ExceptionUtils.getFullStackTrace(e));

      return null;
    }

    if (ipPoint.compareTo(document.get("0")) < 0) {
      LOGGER.debug(ip + " exceeds ipdata record lower limit");

      return null;
    }

    return new Location(
        document.get("2"),
        document.get("3"),
        document.get("4"),
        document.get("5"),
        document.get("6"),
        document.get("7"),
        document.get("8"));
  }

  /**
   * Get location via ip.
   *
   * @param ip ip
   * @return location or null
   */
  public Location getLocation(String ip) {
    if (StringUtils.isEmpty(ip)) {
      return null;
    }

    String ipHex;

    try {
      ipHex = DatatypeConverter.printHexBinary(IPAddressUtil.textToNumericFormatV6(ip));
    } catch (Exception e) {
      LOGGER.debug("ip to hex error: " + ExceptionUtils.getFullStackTrace(e));

      return null;
    }

    libraryReadLock.lock();

    int ipPointIndex = Arrays.binarySearch(ippoints, ipHex);

    if (ipPointIndex < 0) {
      ipPointIndex = Math.abs(ipPointIndex + 1);
    }

    if (ipPointIndex == ippoints.length) {
      LOGGER.debug(ip + " exceeds ipdata upper limit");

      return null;
    }

    String ipPoint = ippoints[ipPointIndex];

    if (useCache) {
      if (!cache.containsKey(ipPoint)) {
        libraryReadLock.unlock();

        libraryWriteLock.lock();

        try {
          if (!cache.containsKey(ipPoint)) {
            cache.put(ipPoint, toLocation(ip, ipPoint));
          }

          libraryReadLock.lock();
        } finally {
          libraryWriteLock.unlock();
        }
      }

      try {
        return cache.get(ipPoint);
      } finally {
        libraryReadLock.unlock();
      }
    } else {
      try {
        return toLocation(ip, ipPoint);
      } finally {
        libraryReadLock.unlock();
      }
    }
  }

  @Override
  public void close() throws IOException {
    executor.shutdownNow();
    while (!executor.isTerminated()) {
      try {
        executor.awaitTermination(1L, TimeUnit.SECONDS);

        LOGGER.info("wait 1 seconds for dynamic loader to be closed ...");
      } catch (InterruptedException e) {
        LOGGER.warn("dynamic loader wait for termination, but interrupted");

        break;
      }
    }

    release();

    LOGGER.info("library closed");
  }
}
