package com.weibo.dip.data.platform.falcon.transport.scribe;

public class ScribeSinkConfigurationConstants {
  /**
   * Maximum number of events the sink should take from the channel per transaction, if available.
   */
  public static final String CONFIG_BATCHSIZE = "batchSize";
  /** The fully qualified class name of the serializer the sink should use. */
  public static final String CONFIG_SERIALIZER = "serializer";
  /** The name the sink should use. */
  public static final String CONFIG_SINK_NAME = "scribe.sink.name";
  /** Scribe host to send to. */
  public static final String CONFIG_SCRIBE_HOST = "scribe.host";
  /** Scribe port to connect to. */
  public static final String CONFIG_SCRIBE_PORT = "scribe.port";
  /** Scribe port to connect to. */
  public static final String CONFIG_SCRIBE_TIMEOUT = "scribe.timeout";
  /** Flume Header Key that maps to a Scribe Category. */
  public static final String CONFIG_SCRIBE_CATEGORY_HEADER = "scribe.category.header";
  /** Flume Header Value that maps to a Scribe Category, used for tests. */
  static final String CONFIG_SCRIBE_CATEGORY = "scribe.category";

  private ScribeSinkConfigurationConstants() {}
}
