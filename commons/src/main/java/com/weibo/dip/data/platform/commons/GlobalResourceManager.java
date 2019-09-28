package com.weibo.dip.data.platform.commons;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global resource manager.
 *
 * @author yurun
 */
public class GlobalResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalResourceManager.class);

  private static final List<Closeable> RESOURCES = new ArrayList<>();

  private GlobalResourceManager() {}

  private static int getIndex(Class<? extends Closeable> clazz) {
    int rindex = -1;

    for (int index = 0; index < RESOURCES.size(); index++) {
      if (Objects.equals(RESOURCES.get(index).getClass(), clazz)) {
        rindex = index;

        break;
      }
    }

    return rindex;
  }

  /**
   * Register resource.
   *
   * @param instance resource instance
   */
  public static void register(Closeable instance) {
    Preconditions.checkState(
        getIndex(instance.getClass()) == -1,
        "%s instance already exists",
        instance.getClass().getName());

    RESOURCES.add(instance);

    LOGGER.info("{} registered", instance.getClass().getName());
  }

  /**
   * Unregister resource.
   *
   * @param clazz resource class
   * @throws IOException if resource close error
   */
  public static void unregister(Class<? extends Closeable> clazz) throws IOException {
    int index = getIndex(clazz);

    Preconditions.checkState(index >= 0, "%s instance does not exist", clazz.getName());

    RESOURCES.remove(index).close();
  }

  /**
   * Get resource.
   *
   * @param clazz resource class
   * @return resource instance
   */
  @SuppressWarnings("unchecked")
  public static <T extends Closeable> T get(Class<T> clazz) {
    int index = getIndex(clazz);

    Preconditions.checkState(index >= 0, "%s instance does not exist", clazz.getName());

    return (T) RESOURCES.get(index);
  }

  /**
   * Close resources.
   *
   * @throws IOException if close resource error
   */
  public static void close() throws IOException {
    for (int index = RESOURCES.size() - 1; index >= 0; index--) {
      Closeable resource = RESOURCES.get(index);

      resource.close();

      LOGGER.info("{} closed", resource.getClass().getName());
    }

    RESOURCES.clear();

    LOGGER.info("global resource manager closed");
  }
}
