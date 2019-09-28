package com.weibo.dip.data.platform.commons.util;

import com.weibo.dip.data.platform.commons.GlobalResourceManager;
import java.io.Closeable;
import java.io.IOException;

/** @author yurun */
public class GlobalResourceManagerTester {
  private static class Resource1 implements Closeable {
    @Override
    public void close() {}
  }

  private static class Resource2 implements Closeable {
    @Override
    public void close() {}
  }

  private static class Resource3 implements Closeable {
    @Override
    public void close() {}
  }

  public static void main(String[] args) throws IOException {
    GlobalResourceManager.register(new Resource1());
    GlobalResourceManager.register(new Resource2());
    // GlobalResourceManager.register(new Resource1());

    Resource2 resource2 = GlobalResourceManager.get(Resource2.class);
    // Resource3 resource3 = GlobalResourceManager.get(Resource3.class);

    GlobalResourceManager.close();
  }
}
