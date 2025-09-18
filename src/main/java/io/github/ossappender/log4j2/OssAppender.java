package io.github.ossappender.log4j2;

// 改为依赖公共 core 组件
import io.github.ossappender.core.BatchingQueue;
import io.github.ossappender.core.OssUploader;
import io.github.ossappender.core.UploadHooks;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.util.Throwables;

import java.io.Serializable;
import java.util.Objects;

/**
 * 高性能 Log4j2 Appender：将日志实时上传至阿里云 OSS。
 *
 * 功能：
 * 1. 内存队列 + 后台线程异步批量上传
 * 2. 支持批量条数、批量字节、定时刷新
 * 3. 支持 gzip 压缩、失败重试与简单退避
 * 4. 阻塞/丢弃策略可配置
 */
@Plugin(name = "OssAppender", category = "Core", elementType = "appender", printObject = true)
public class OssAppender extends AbstractAppender {

    private final boolean blockWhenQueueFull;
    private final UploadHooks hooks;
    private final Object queueImpl; // BatchingQueue 或 DisruptorBatchingQueue
    private final OssUploader uploader;

    protected OssAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                          boolean ignoreExceptions, Property[] properties,
                          int queueSize, int maxBatchMessages, int maxBatchBytes,
                          long flushIntervalMillis, boolean gzipEnabled,
                          boolean blockWhenQueueFull, int maxRetry, long baseBackoffMillis,
                          String endpoint, String accessKeyId, String accessKeySecret,
                          String bucket, String objectPrefix,
                          boolean useDisruptor, boolean multiProducer) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.blockWhenQueueFull = blockWhenQueueFull;
        this.hooks = UploadHooks.noop();
        this.uploader = new OssUploader(
                endpoint, accessKeyId, accessKeySecret,
                bucket, objectPrefix == null ? "logs" : objectPrefix,
                gzipEnabled, maxRetry, baseBackoffMillis, 30_000L,
                hooks
        );
        if (useDisruptor) {
            io.github.ossappender.core.DisruptorBatchingQueue dq = new io.github.ossappender.core.DisruptorBatchingQueue(
                    nearestPowerOfTwo(queueSize), maxBatchMessages, maxBatchBytes, flushIntervalMillis,
                    blockWhenQueueFull, multiProducer,
                    (events, totalBytes) -> { uploader.uploadBatch(events, totalBytes); return true; }
            );
            dq.start();
            this.queueImpl = dq;
        } else {
            BatchingQueue bq = new BatchingQueue(
                    queueSize, maxBatchMessages, maxBatchBytes, flushIntervalMillis,
                    blockWhenQueueFull,
                    (events, totalBytes) -> { uploader.uploadBatch(events, totalBytes); return true; }
            );
            bq.start();
            this.queueImpl = bq;
        }
    }

    /**
     * 将单条日志事件序列化为字节并进入内存队列。
     * - 队列满时，根据 blockWhenQueueFull 决定阻塞或丢弃。
     */
    @Override
    public void append(LogEvent event) {
        try {
            byte[] bytes = getLayout().toByteArray(event);
            boolean accepted = (queueImpl instanceof BatchingQueue)
                    ? ((BatchingQueue) queueImpl).offer(bytes)
                    : ((io.github.ossappender.core.DisruptorBatchingQueue) queueImpl).offer(bytes);
            if (!accepted && !blockWhenQueueFull) {
                hooks.onDropped(bytes.length, -1);
            }
        } catch (Throwable t) {
            try {
                org.apache.logging.log4j.LogManager.getLogger(OssAppender.class)
                        .warn("Failed to append log event to queue", t);
            } catch (Throwable ignore) {
                // ensure never affect business thread
            }
        }
    }

    /**
     * 初始化 OSS 客户端并启动后台消费线程。
     */
    @Override
    public void start() { super.start(); }

    /**
     * 优雅关闭：停止线程，刷出剩余批次并关闭 OSS 客户端。
     */
    @Override
    public void stop() {
        try {
            if (queueImpl instanceof BatchingQueue) ((BatchingQueue) queueImpl).close();
            else ((io.github.ossappender.core.DisruptorBatchingQueue) queueImpl).close();
        } catch (Throwable t) {
            try { org.apache.logging.log4j.LogManager.getLogger(OssAppender.class).warn("Failed to close queue", t); } catch (Throwable ignore) {}
        }
        try { uploader.close(); } catch (Throwable t) {
            try { org.apache.logging.log4j.LogManager.getLogger(OssAppender.class).warn("Failed to close uploader", t); } catch (Throwable ignore) {}
        }
        super.stop();
    }

    private static int nearestPowerOfTwo(int n) {
        int x = 1;
        while (x < n) x <<= 1;
        return x;
    }

    /**
     * 后台线程主循环：按条数/字节/时间阈值聚合并触发上传。
     */
    // 旧的 drainLoop / 上传逻辑由通用组件替代

    // 不再需要 flushOnce，批处理由 BatchingQueue 管理

    /**
     * 执行批量上传：合并为 JSON Lines 文本，可选 gzip；失败按指数退避重试。
     */
    // 由通用 uploader 处理

    /**
     * 将多条日志字节以换行符拼接成 JSON Lines。
     */
    // 由通用 uploader 处理

    /**
     * 对给定字节数组进行 gzip 压缩。
     */
    // 由通用 uploader 处理

    /**
     * 生成对象键：前缀 + 时间戳 + UUID + 后缀。
     */
    // 由通用 uploader 处理

    /**
     * Appender Builder。支持在 log4j2.xml 中配置。
     */
    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<OssAppender> {
        @PluginAttribute("name")
        private String name;
        @PluginElement("Filter")
        private Filter filter;
        @PluginElement("Layout")
        private Layout<? extends Serializable> layout;

        // 队列/批处理
        @PluginAttribute("queueSize")
        private int queueSize = 65536;
        @PluginAttribute("maxBatchMessages")
        private int maxBatchMessages = 500;
        @PluginAttribute("maxBatchBytes")
        private int maxBatchBytes = 512 * 1024;
        @PluginAttribute("flushIntervalMillis")
        private long flushIntervalMillis = 1000;
        @PluginAttribute("gzipEnabled")
        private boolean gzipEnabled = true;
        @PluginAttribute("blockWhenQueueFull")
        private boolean blockWhenQueueFull = true;

        // 重试/退避
        @PluginAttribute("maxRetry")
        private int maxRetry = 5;
        @PluginAttribute("baseBackoffMillis")
        private long baseBackoffMillis = 200L;

        // OSS 参数
        @PluginAttribute("endpoint")
        private String endpoint;
        @PluginAttribute("accessKeyId")
        private String accessKeyId;
        @PluginAttribute("accessKeySecret")
        private String accessKeySecret;
        @PluginAttribute("bucket")
        private String bucket;
        @PluginAttribute("objectPrefix")
        private String objectPrefix = "logs/";
        @PluginAttribute("useDisruptor")
        private boolean useDisruptor = false;
        @PluginAttribute("multiProducer")
        private boolean multiProducer = true;

        @Override
        public OssAppender build() {
            if (layout == null) {
                layout = org.apache.logging.log4j.core.layout.PatternLayout.newBuilder()
                        .withPattern("%d{ISO8601} %level %logger - %msg%n")
                        .build();
            }
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(endpoint, "endpoint");
            Objects.requireNonNull(accessKeyId, "accessKeyId");
            Objects.requireNonNull(accessKeySecret, "accessKeySecret");
            Objects.requireNonNull(bucket, "bucket");
            return new OssAppender(name, filter, layout, true, Property.EMPTY_ARRAY,
                    queueSize, maxBatchMessages, maxBatchBytes, flushIntervalMillis,
                    gzipEnabled, blockWhenQueueFull, maxRetry, baseBackoffMillis,
                    endpoint, accessKeyId, accessKeySecret, bucket, objectPrefix,
                    useDisruptor, multiProducer);
        }

        // 以下为链式 setter，便于程序化配置
        public Builder setName(String name) { this.name = name; return this; }
        public Builder setFilter(Filter filter) { this.filter = filter; return this; }
        public Builder setLayout(Layout<? extends Serializable> layout) { this.layout = layout; return this; }
        public Builder setQueueSize(int queueSize) { this.queueSize = queueSize; return this; }
        public Builder setMaxBatchMessages(int v) { this.maxBatchMessages = v; return this; }
        public Builder setMaxBatchBytes(int v) { this.maxBatchBytes = v; return this; }
        public Builder setFlushIntervalMillis(long v) { this.flushIntervalMillis = v; return this; }
        public Builder setGzipEnabled(boolean v) { this.gzipEnabled = v; return this; }
        public Builder setBlockWhenQueueFull(boolean v) { this.blockWhenQueueFull = v; return this; }
        public Builder setMaxRetry(int v) { this.maxRetry = v; return this; }
        public Builder setBaseBackoffMillis(long v) { this.baseBackoffMillis = v; return this; }
        public Builder setEndpoint(String v) { this.endpoint = v; return this; }
        public Builder setAccessKeyId(String v) { this.accessKeyId = v; return this; }
        public Builder setAccessKeySecret(String v) { this.accessKeySecret = v; return this; }
        public Builder setBucket(String v) { this.bucket = v; return this; }
        public Builder setObjectPrefix(String v) { this.objectPrefix = v; return this; }
    }
}


