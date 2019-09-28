package com.weibo.dip.data.platform.commons.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yurun
 * @datetime 2014-8-1 上午10:06:28
 */
public class HDFSUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSUtil.class);

  private static final Configuration CONFIGURATION;

  private static final FileSystem FILE_SYSTEM;

  static {
    try {
      CONFIGURATION = HadoopConfiguration.getInstance();

      FILE_SYSTEM = FileSystem.get(CONFIGURATION);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static Configuration getConfiguration() {
    return CONFIGURATION;
  }

  public static FileSystem getFileSystem() {
    return FILE_SYSTEM;
  }

  /**
   * 指定路径是否存在
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean exist(Path path) throws IOException {
    return FILE_SYSTEM.exists(path);
  }

  /**
   * 指定路径是否存在
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean exist(String path) throws IOException {
    return exist(new Path(path));
  }

  /**
   * 指定路径是否为文件
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean isFile(Path path) throws IOException {
    return FILE_SYSTEM.isFile(path);
  }

  /**
   * 指定路径是否为文件
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean isFile(String path) throws IOException {
    return isFile(new Path(path));
  }

  /**
   * 指定路径是否为目录
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean isDirectory(Path path) throws IOException {
    return FILE_SYSTEM.isDirectory(path);
  }

  /**
   * 指定路径是否为目录
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean isDirectory(String path) throws IOException {
    return isDirectory(new Path(path));
  }

  /**
   * 递归（非递归）方式列出某一目录下的所有文件
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listFiles(Path path, boolean recursive)
      throws FileNotFoundException, IOException {
    if (!exist(path) || !isDirectory(path)) {
      LOGGER.error(path + " not exist or not a dir");

      return null;
    }

    RemoteIterator<LocatedFileStatus> iterator = FILE_SYSTEM.listFiles(path, recursive);

    List<Path> files = new ArrayList<Path>();

    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();

      files.add(fileStatus.getPath());
    }

    return files;
  }

  /**
   * 递归（非递归）方式列出某一目录下的所有文件
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listFiles(String path, boolean recursive)
      throws FileNotFoundException, IOException {
    return listFiles(new Path(path), recursive);
  }

  /**
   * 递归（非递归）方式列出某一目录下满足过滤条件的所有文件
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws IOException
   */
  public static List<Path> listFiles(Path path, boolean recursive, PathFilter pathFilter)
      throws IOException {
    if (!exist(path) || !isDirectory(path)) {
      LOGGER.error(path + " not exist or not a dir");

      return null;
    }

    FileStatus[] status = FILE_SYSTEM.listStatus(path, pathFilter);

    if (status == null || status.length <= 0) {
      return null;
    }

    Path[] paths = FileUtil.stat2Paths(status);

    List<Path> files = new ArrayList<Path>();

    for (Path file : paths) {
      if (isFile(file)) {
        files.add(file);
      } else {
        List<Path> subFiles = listFiles(file, recursive, pathFilter);

        if (CollectionUtils.isNotEmpty(subFiles)) {
          files.addAll(subFiles);
        }
      }
    }

    return files;
  }

  /**
   * 递归（非递归）方式列出某一目录下满足过滤条件的所有文件
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws IOException
   */
  public static List<Path> listFiles(String path, boolean recursive, PathFilter pathFilter)
      throws IOException {
    return listFiles(new Path(path), recursive, pathFilter);
  }

  /**
   * 递归（非递归）方式列出某一目录下的所有目录
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listDirs(Path path, boolean recursive)
      throws FileNotFoundException, IOException {
    return listDirs(path, recursive, null);
  }

  /**
   * 递归（非递归）方式列出某一目录下的所有目录
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listDirs(String path, boolean recursive)
      throws FileNotFoundException, IOException {
    return listDirs(new Path(path), recursive);
  }

  /**
   * 递归（非递归）方式列出某一目录下满足所有条件的所有目录
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listDirs(Path path, boolean recursive, PathFilter pathFilter)
      throws FileNotFoundException, IOException {
    if (!exist(path) || !isDirectory(path)) {
      LOGGER.error(path + " not exist or not a dir");

      return null;
    }

    FileStatus[] status;

    if (Objects.isNull(pathFilter)) {
      status = FILE_SYSTEM.listStatus(path);
    } else {
      status = FILE_SYSTEM.listStatus(path, pathFilter);
    }

    if (ArrayUtils.isEmpty(status)) {
      return null;
    }

    List<Path> dirs = new ArrayList<>();

    for (FileStatus fstatus : status) {
      if (fstatus.isDirectory()) {
        dirs.add(fstatus.getPath());

        if (recursive) {
          List<Path> subDirs = listDirs(fstatus.getPath(), recursive, pathFilter);
          if (CollectionUtils.isNotEmpty(subDirs)) {
            dirs.addAll(subDirs);
          }
        }
      }
    }

    return dirs;
  }

  /**
   * 递归（非递归）方式列出某一目录下满足所有条件的所有目录
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static List<Path> listDirs(String path, boolean recursive, PathFilter pathFilter)
      throws FileNotFoundException, IOException {
    return listDirs(new Path(path), recursive, pathFilter);
  }

  /**
   * 获取指定文件的大小
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static long getFileSize(Path path) throws IOException {
    if (!exist(path) || !isFile(path)) {
      LOGGER.error(path + " not exist or not a file");

      return -1;
    }

    return FILE_SYSTEM.getFileStatus(path).getLen();
  }

  /**
   * 获取指定文件的大小
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static long getFileSize(String path) throws IOException {
    return getFileSize(new Path(path));
  }

  /**
   * 获取指定（批）文件的大小
   *
   * @param paths
   * @return
   * @throws IOException
   */
  public static long getFileSize(List<Path> paths) throws IOException {
    if (paths == null || paths.isEmpty()) {
      return 0;
    }

    long fileSumSize = 0;

    for (Path path : paths) {
      long fileSize = getFileSize(path);

      if (fileSize == -1) {
        return -1;
      }

      fileSumSize += fileSize;
    }

    return fileSumSize;
  }

  /**
   * 获取指定目录下（递归或非递归）所有文件的大小总和
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static long getFileSize(Path path, boolean recursive)
      throws FileNotFoundException, IOException {
    long total = 0;

    List<Path> files = listFiles(path, recursive);
    if (files == null || files.isEmpty()) {
      return total;
    }

    for (Path file : files) {
      total += getFileSize(file);
    }

    return total;
  }

  /**
   * 获取指定目录下（递归或非递归）所有文件的大小总和
   *
   * @param path
   * @param recursive
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static long getFileSize(String path, boolean recursive)
      throws FileNotFoundException, IOException {
    return getFileSize(new Path(path), recursive);
  }

  /**
   * 获取指定目录下（递归或非递归）满足过滤条件的所有文件的大小总和
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws IOException
   */
  public static long getFileSize(Path path, boolean recursive, PathFilter pathFilter)
      throws IOException {
    long total = 0;

    List<Path> files = listFiles(path, recursive, pathFilter);
    if (files == null || files.isEmpty()) {
      return total;
    }

    for (Path file : files) {
      total += getFileSize(file);
    }

    return total;
  }

  /**
   * 获取指定目录下（递归或非递归）满足过滤条件的所有文件的大小总和
   *
   * @param path
   * @param recursive
   * @param pathFilter
   * @return
   * @throws IOException
   */
  public static long getFileSize(String path, boolean recursive, PathFilter pathFilter)
      throws IOException {
    return getFileSize(new Path(path), recursive, pathFilter);
  }

  /**
   * 打开指定文件的输入流
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static FSDataInputStream openInputStream(Path path) throws IOException {
    return FILE_SYSTEM.open(path);
  }

  /**
   * 打开指定文件的输入流
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static FSDataInputStream openInputStream(String path) throws IOException {
    return openInputStream(new Path(path));
  }

  /**
   * 关闭指定输入流
   *
   * @param in
   */
  public static void closeInputStream(FSDataInputStream in) {
    IOUtils.closeQuietly(in);
  }

  /**
   * 打开指定文件的输出流
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static FSDataOutputStream openOutputStream(Path path) throws IOException {
    return FILE_SYSTEM.create(path);
  }

  /**
   * 打开指定文件的输出流
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static FSDataOutputStream openOutputStream(String path) throws IOException {
    return openOutputStream(new Path(path));
  }

  /**
   * 关闭指定输出流
   *
   * @param out
   */
  public static void closeOutputStream(FSDataOutputStream out) {
    IOUtils.closeQuietly(out);
  }

  public static CompressionCodec getCompressionCodec(String className)
      throws ClassNotFoundException {
    Class<?> codecClass = Class.forName(className);

    return (CompressionCodec) ReflectionUtils.newInstance(codecClass, CONFIGURATION);
  }

  /**
   * 删除指定文件
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean deleteFile(Path path) throws IOException {
    if (exist(path) && isFile(path)) {
      return FILE_SYSTEM.delete(path, false);
    }

    return false;
  }

  /**
   * 删除指定文件
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean deleteFile(String path) throws IOException {
    return deleteFile(new Path(path));
  }

  /**
   * 删除指定目录
   *
   * @param path
   * @param recursive
   * @return
   * @throws IOException
   */
  public static boolean deleteDir(Path path, boolean recursive) throws IOException {
    if (exist(path) && isDirectory(path)) {
      return FILE_SYSTEM.delete(path, recursive);
    }

    return false;
  }

  /**
   * 删除指定目录
   *
   * @param path
   * @param recursive
   * @return
   * @throws IOException
   */
  public static boolean deleteDir(String path, boolean recursive) throws IOException {
    return deleteDir(new Path(path), recursive);
  }

  /**
   * 创建指定名称的目录
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean mkdir(Path path) throws IOException {
    return FILE_SYSTEM.mkdirs(path);
  }

  /**
   * 创建指定名称的目录
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static boolean mkdir(String path) throws IOException {
    return mkdir(new Path(path));
  }

  /**
   * 返回txt格式的文件流，压缩格式使用codec转换
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static InputStream openInputStreamIgnoreCompress(String path) throws IOException {
    return openInputStreamIgnoreCompress(new Path(path));
  }

  /**
   * 返回txt格式的文件流，压缩格式使用codec转换
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static InputStream openInputStreamIgnoreCompress(Path path) throws IOException {
    InputStream inputStream = null;

    CompressionCodecFactory factory = new CompressionCodecFactory(CONFIGURATION);
    CompressionCodec codec = factory.getCodec(path);

    if (codec == null) {
      inputStream = FILE_SYSTEM.open(path);
    } else {
      inputStream = codec.createInputStream(FILE_SYSTEM.open(path));
    }
    return inputStream;
  }

  public static ContentSummary summary(Path path) throws IOException {
    if (exist(path)) {
      return FILE_SYSTEM.getContentSummary(path);
    }

    return null;
  }

  public static ContentSummary summary(String path) throws IOException {
    return summary(new Path(path));
  }

  public static void copyFromLocal(Path src, Path dst) throws IOException {
    FILE_SYSTEM.copyFromLocalFile(src, dst);
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {}
}
