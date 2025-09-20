package io.github.ossappender.log4j2;

import io.github.ossappender.adapter.S3Log4j2Adapter;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Objects;

/**
 * S3兼容对象存储 Log4j2 Appender：
 * - 支持AWS S3、阿里云OSS、腾讯云COS、MinIO、Cloudflare R2等所有S3兼容存储
 * - 基于AWS SDK v2构建，提供统一的对象存储接口
 * - 继承 AbstractAppender 提供Log4j2标准接口
 * - 核心逻辑委托给 S3Log4j2Adapter（复用log-java-producer的高性能组件）
 */
@Plugin(name = "S3Appender", category = "Core", elementType = "appender", printObject = true)
public final class S3Appender extends AbstractAppender {

    private S3Log4j2Adapter adapter;

    protected S3Appender(String name, Filter filter, Layout<? extends Serializable> layout,
                         boolean ignoreExceptions, Property[] properties, S3Log4j2Adapter adapter) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.adapter = adapter;
    }

    @Override
    public void append(LogEvent event) {
        if (!isStarted() || adapter == null) {
            return;
        }
        try {
            // Log4j2 layout 序列化为字符串
            String logLine = getLayout().toSerializable(event).toString();
            adapter.offer(logLine);
        } catch (Exception e) {
            error("Failed to append log event", event, e);
        }
    }

    @Override
    public boolean stop(long timeout, java.util.concurrent.TimeUnit timeUnit) {
        setStopping();
        try {
            if (adapter != null) {
                adapter.close();
            }
        } catch (Exception e) {
            error("Failed to gracefully close adapter", e);
        }
        setStopped();
        return true;
    }

    @PluginBuilderFactory
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<S3Appender> {
        @PluginAttribute("name")
        private String name;

        @PluginElement("Filter")
        private Filter filter;

        @PluginElement("Layout")
        private Layout<? extends Serializable> layout;

        // S3兼容存储配置 - 必需参数
        @PluginAttribute("endpoint")
        private String endpoint;

        @PluginAttribute("region")
        private String region;

        @PluginAttribute("accessKeyId")
        private String accessKeyId;

        @PluginAttribute("accessKeySecret")
        private String accessKeySecret;

        @PluginAttribute("bucket")
        private String bucket;

        // 应用行为配置 - 可选参数，提供最优默认值
        @PluginAttribute("keyPrefix")
        private String keyPrefix = "logs/";

        @PluginAttribute("maxQueueSize")
        private int maxQueueSize = 200_000;

        @PluginAttribute("maxBatchCount")
        private int maxBatchCount = 5_000;

        @PluginAttribute("maxBatchBytes")
        private int maxBatchBytes = 4 * 1024 * 1024;

        @PluginAttribute("flushIntervalMs")
        private long flushIntervalMs = 2000L;

        @PluginAttribute("dropWhenQueueFull")
        private boolean dropWhenQueueFull = false;

        @PluginAttribute("multiProducer")
        private boolean multiProducer = false;

        @PluginAttribute("maxRetries")
        private int maxRetries = 5;

        @PluginAttribute("baseBackoffMs")
        private long baseBackoffMs = 200L;

        @PluginAttribute("maxBackoffMs")
        private long maxBackoffMs = 10000L;

        @Override
        public S3Appender build() {
            if (layout == null) {
                layout = PatternLayout.newBuilder()
                        .withPattern("%d{ISO8601} [%t] %-5level %logger{36} - %msg%n")
                        .build();
            }

            // 验证必需参数
            Objects.requireNonNull(name, "name must be set");
            Objects.requireNonNull(accessKeyId, "accessKeyId must be set");
            Objects.requireNonNull(accessKeySecret, "accessKeySecret must be set");
            Objects.requireNonNull(bucket, "bucket must be set");

            try {
                // 构建S3Log4j2Adapter配置
                S3Log4j2Adapter.Config config = new S3Log4j2Adapter.Config();
                config.endpoint = this.endpoint;
                config.region = this.region;
                config.accessKeyId = this.accessKeyId;
                config.accessKeySecret = this.accessKeySecret;
                config.bucket = this.bucket;
                config.keyPrefix = this.keyPrefix;
                config.maxQueueSize = this.maxQueueSize;
                config.maxBatchCount = this.maxBatchCount;
                config.maxBatchBytes = this.maxBatchBytes;
                config.flushIntervalMs = this.flushIntervalMs;
                config.dropWhenQueueFull = this.dropWhenQueueFull;
                config.multiProducer = this.multiProducer;
                config.maxRetries = this.maxRetries;
                config.baseBackoffMs = this.baseBackoffMs;
                config.maxBackoffMs = this.maxBackoffMs;

                S3Log4j2Adapter adapter = new S3Log4j2Adapter(config);
                return new S3Appender(name, filter, layout, true, Property.EMPTY_ARRAY, adapter);

            } catch (Exception e) {
                throw new RuntimeException("Failed to build S3Appender: " + e.getMessage(), e);
            }
        }

        // region setters for Log4j2 config
        public Builder setName(String name) { this.name = name; return this; }
        public Builder setFilter(Filter filter) { this.filter = filter; return this; }
        public Builder setLayout(Layout<? extends Serializable> layout) { this.layout = layout; return this; }
        public Builder setEndpoint(String endpoint) { this.endpoint = endpoint; return this; }
        public Builder setRegion(String region) { this.region = region; return this; }
        public Builder setAccessKeyId(String accessKeyId) { this.accessKeyId = accessKeyId; return this; }
        public Builder setAccessKeySecret(String accessKeySecret) { this.accessKeySecret = accessKeySecret; return this; }
        public Builder setBucket(String bucket) { this.bucket = bucket; return this; }
        public Builder setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }
        public Builder setMaxQueueSize(int maxQueueSize) { this.maxQueueSize = maxQueueSize; return this; }
        public Builder setMaxBatchCount(int maxBatchCount) { this.maxBatchCount = maxBatchCount; return this; }
        public Builder setMaxBatchBytes(int maxBatchBytes) { this.maxBatchBytes = maxBatchBytes; return this; }
        public Builder setFlushIntervalMs(long flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; return this; }
        public Builder setDropWhenQueueFull(boolean dropWhenQueueFull) { this.dropWhenQueueFull = dropWhenQueueFull; return this; }
        public Builder setMultiProducer(boolean multiProducer) { this.multiProducer = multiProducer; return this; }
        public Builder setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder setBaseBackoffMs(long baseBackoffMs) { this.baseBackoffMs = baseBackoffMs; return this; }
        public Builder setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; return this; }
        // endregion
    }
}