package io.github.ossappender.adapter;

import io.github.ossappender.core.DisruptorBatchingQueue;
import io.github.ossappender.s3.S3CompatibleUploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 高性能S3兼容日志适配器：专为Log4j2设计，复用log-java-producer的核心组件
 * - 基于DisruptorBatchingQueue实现无锁高吞吐
 * - 集成S3CompatibleUploader支持所有S3兼容存储
 * - 极简封装，专注Log4j2集成
 */
public final class S3Log4j2Adapter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(S3Log4j2Adapter.class);

    private final DisruptorBatchingQueue queue;
    private final S3CompatibleUploader uploader;

    /**
     * 配置参数
     */
    public static class Config {
        // S3配置
        public String endpoint;
        public String region;
        public String accessKeyId;
        public String accessKeySecret;
        public String bucket;
        public String keyPrefix = "logs/";

        // 队列配置
        public int maxQueueSize = 200_000;
        public int maxBatchCount = 5_000;
        public int maxBatchBytes = 4 * 1024 * 1024;
        public long flushIntervalMs = 2000L;
        public boolean dropWhenQueueFull = false;
        public boolean multiProducer = false;

        // 上传配置
        public int maxRetries = 5;
        public long baseBackoffMs = 200L;
        public long maxBackoffMs = 10000L;
    }

    /**
     * 构造适配器
     */
    public S3Log4j2Adapter(Config config) {
        try {
            // 创建S3上传器
            this.uploader = new S3CompatibleUploader(
                config.endpoint,
                config.region,
                config.accessKeyId,
                config.accessKeySecret,
                config.bucket,
                config.keyPrefix,
                config.maxRetries,
                config.baseBackoffMs,
                config.maxBackoffMs,
                false  // forcePathStyle - false为默认值，适用于大多数S3兼容服务
            );

            // 创建高性能队列，使用批次消费回调
            this.queue = new DisruptorBatchingQueue(
                config.maxQueueSize,
                config.maxBatchCount,
                config.maxBatchBytes,
                config.flushIntervalMs,
                config.dropWhenQueueFull,
                config.multiProducer,
                this::onBatch
            );

            // 启动队列
            this.queue.start();

            logger.info("S3Log4j2Adapter initialized for bucket: {}", config.bucket);

        } catch (Exception e) {
            logger.error("Failed to initialize S3Log4j2Adapter", e);
            throw new RuntimeException("S3Log4j2Adapter initialization failed", e);
        }
    }

    /**
     * 提交日志行
     */
    public boolean offer(String logLine) {
        if (logLine == null || logLine.trim().isEmpty()) {
            return false;
        }

        // 确保以换行符结尾（NDJSON格式）
        String line = logLine.endsWith("\n") ? logLine : logLine + "\n";
        return queue.offer(line.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * 批次处理回调：将日志批次上传到S3
     */
    private boolean onBatch(java.util.List<DisruptorBatchingQueue.LogEvent> events, int totalBytes) {
        try {
            // 编码为NDJSON
            byte[] ndjson = encodeNdjson(events);

            // 上传到S3（自动压缩、重试等由uploader处理）
            uploader.upload(null, ndjson, "application/x-ndjson", "gzip");

            return true;
        } catch (Exception e) {
            logger.error("Failed to upload batch to S3, events: {}, bytes: {}",
                        events.size(), totalBytes, e);
            return false;
        }
    }

    /**
     * 编码为NDJSON格式
     */
    private byte[] encodeNdjson(java.util.List<DisruptorBatchingQueue.LogEvent> events) {
        try (java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(
                Math.max(256, events.size() * 128))) {

            for (DisruptorBatchingQueue.LogEvent event : events) {
                if (event.payload != null) {
                    out.write(event.payload);
                    // 每个事件的payload已经包含换行符，无需重复添加
                }
            }
            return out.toByteArray();

        } catch (java.io.IOException e) {
            logger.warn("Failed to encode NDJSON", e);
            return new byte[0];
        }
    }

    @Override
    public void close() {
        try {
            if (queue != null) {
                queue.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close queue", e);
        }

        try {
            if (uploader != null) {
                uploader.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close uploader", e);
        }
    }
}