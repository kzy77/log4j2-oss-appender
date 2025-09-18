package io.github.ossappender.log4j2;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ObjectMetadata;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

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

    private final ArrayBlockingQueue<byte[]> queue;
    private final ExecutorService executor;
    private final int maxBatchMessages;
    private final int maxBatchBytes;
    private final long flushIntervalMillis;
    private final boolean gzipEnabled;
    private final boolean blockWhenQueueFull;
    private final int maxRetry;
    private final long baseBackoffMillis;

    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String bucket;
    private final String objectPrefix;

    private volatile boolean running = true;
    private volatile OSS ossClient;

    protected OssAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                          boolean ignoreExceptions, Property[] properties,
                          int queueSize, int maxBatchMessages, int maxBatchBytes,
                          long flushIntervalMillis, boolean gzipEnabled,
                          boolean blockWhenQueueFull, int maxRetry, long baseBackoffMillis,
                          String endpoint, String accessKeyId, String accessKeySecret,
                          String bucket, String objectPrefix) {
        super(name, filter, layout, ignoreExceptions, properties);
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "log4j2-oss-uploader");
            t.setDaemon(true);
            return t;
        });
        this.maxBatchMessages = maxBatchMessages;
        this.maxBatchBytes = maxBatchBytes;
        this.flushIntervalMillis = flushIntervalMillis;
        this.gzipEnabled = gzipEnabled;
        this.blockWhenQueueFull = blockWhenQueueFull;
        this.maxRetry = maxRetry;
        this.baseBackoffMillis = baseBackoffMillis;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.bucket = bucket;
        this.objectPrefix = objectPrefix == null ? "logs/" : objectPrefix;
    }

    /**
     * 将单条日志事件序列化为字节并进入内存队列。
     * - 队列满时，根据 blockWhenQueueFull 决定阻塞或丢弃。
     */
    @Override
    public void append(LogEvent event) {
        try {
            byte[] bytes = getLayout().toByteArray(event);
            if (blockWhenQueueFull) {
                queue.put(bytes);
            } else {
                queue.offer(bytes); // 丢弃策略：队列满时丢弃
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            if (!ignoreExceptions()) {
                Throwables.rethrow(t);
            }
        }
    }

    /**
     * 初始化 OSS 客户端并启动后台消费线程。
     */
    @Override
    public void start() {
        super.start();
        this.ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
        executor.submit(this::drainLoop);
    }

    /**
     * 优雅关闭：停止线程，刷出剩余批次并关闭 OSS 客户端。
     */
    @Override
    public void stop() {
        running = false;
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 尝试同步刷出剩余数据
        flushOnce(true);
        if (ossClient != null) {
            ossClient.shutdown();
        }
        super.stop();
    }

    /**
     * 后台线程主循环：按条数/字节/时间阈值聚合并触发上传。
     */
    private void drainLoop() {
        long lastFlush = System.currentTimeMillis();
        Queue<byte[]> batch = new ArrayDeque<>(maxBatchMessages);
        int batchBytes = 0;
        try {
            while (running) {
                long wait = flushIntervalMillis - (System.currentTimeMillis() - lastFlush);
                if (wait <= 0) wait = 50L;
                byte[] item = queue.poll(wait, TimeUnit.MILLISECONDS);
                if (item != null) {
                    batch.add(item);
                    batchBytes += item.length;
                }
                boolean timeUp = (System.currentTimeMillis() - lastFlush) >= flushIntervalMillis;
                boolean sizeReached = batch.size() >= maxBatchMessages || batchBytes >= maxBatchBytes;
                if (timeUp || sizeReached) {
                    if (!batch.isEmpty()) {
                        uploadBatch(batch);
                        batch.clear();
                        batchBytes = 0;
                    }
                    lastFlush = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 退出前最后刷出
            if (!batch.isEmpty()) {
                uploadBatch(batch);
            }
        }
    }

    /**
     * 在停止阶段同步尝试刷出队列中的剩余数据。
     */
    private void flushOnce(boolean drainAll) {
        Queue<byte[]> batch = new ArrayDeque<>(maxBatchMessages);
        int batchBytes = 0;
        long start = System.currentTimeMillis();
        while (!queue.isEmpty() && (drainAll || (System.currentTimeMillis() - start) < 1000)) {
            byte[] item = queue.poll();
            if (item == null) break;
            batch.add(item);
            batchBytes += item.length;
            if (batch.size() >= maxBatchMessages || batchBytes >= maxBatchBytes) {
                uploadBatch(batch);
                batch.clear();
                batchBytes = 0;
            }
        }
        if (!batch.isEmpty()) {
            uploadBatch(batch);
        }
    }

    /**
     * 执行批量上传：合并为 JSON Lines 文本，可选 gzip；失败按指数退避重试。
     */
    private void uploadBatch(Queue<byte[]> batch) {
        if (batch.isEmpty()) return;
        try {
            byte[] payload = joinWithNewline(batch);
            if (gzipEnabled) {
                payload = gzip(payload);
            }
            String objectKey = buildObjectKey();
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(payload.length);
            meta.setContentType("application/json; charset=utf-8");
            if (gzipEnabled) meta.setContentEncoding("gzip");

            int attempt = 0;
            while (true) {
                try {
                    ossClient.putObject(bucket, objectKey, new ByteArrayInputStream(payload), meta);
                    break;
                } catch (Throwable t) {
                    attempt++;
                    if (attempt > maxRetry) throw t;
                    Thread.sleep(Math.min(30_000L, baseBackoffMillis * (1L << (attempt - 1))));
                }
            }
        } catch (Throwable t) {
            if (!ignoreExceptions()) {
                Throwables.rethrow(t);
            }
        }
    }

    /**
     * 将多条日志字节以换行符拼接成 JSON Lines。
     */
    private static byte[] joinWithNewline(Queue<byte[]> batch) {
        int size = 0;
        for (byte[] b : batch) size += b.length + 1;
        ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        int i = 0, n = batch.size();
        for (byte[] b : batch) {
            out.write(b, 0, b.length);
            if (++i < n) out.write('\n');
        }
        return out.toByteArray();
    }

    /**
     * 对给定字节数组进行 gzip 压缩。
     */
    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(256, data.length / 4));
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(data);
        gzip.finish();
        gzip.close();
        return out.toByteArray();
    }

    /**
     * 生成对象键：前缀 + 时间戳 + UUID + 后缀。
     */
    private String buildObjectKey() {
        Instant now = Instant.now();
        String ts = String.valueOf(now.toEpochMilli());
        String uuid = UUID.randomUUID().toString();
        return objectPrefix + ts + "-" + uuid + (gzipEnabled ? ".log.gz" : ".log");
    }

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
                    endpoint, accessKeyId, accessKeySecret, bucket, objectPrefix);
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


