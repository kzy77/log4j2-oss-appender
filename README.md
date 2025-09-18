# log4j2-oss-appender

高性能 Log4j2 Appender，将应用日志实时上传至阿里云 OSS。

特性：
- 不落盘：日志直接入内存队列并异步上传
- 无需改造：仅配置即可接入
- 异步高吞吐：批处理、压缩、连接复用
- 不丢日志：可配置为生产侧阻塞等待，钩子监听，确保写入
- 可配置：队列大小、批量条数/字节、刷新间隔、重试与退避、gzip 等

## 安装

Maven（示例）：
```xml
<dependency>
  <groupId>io.github.ossappender</groupId>
  <artifactId>log4j2-oss-appender</artifactId>
  <version>0.1.0</version>
 </dependency>
```

## 快速开始（log4j2.xml）

将下列片段加入你的 `log4j2.xml`：
```xml
<Configuration packages="io.github.ossappender.log4j2">
  <Appenders>
    <OssAppender name="oss" endpoint="https://oss-cn-hangzhou.aliyuncs.com"
                 accessKeyId="${sys:ALIYUN_AK}" accessKeySecret="${sys:ALIYUN_SK}"
                 bucket="your-bucket" objectPrefix="app-logs/"
                 queueSize="65536" maxBatchMessages="500" maxBatchBytes="524288"
                 flushIntervalMillis="1000" gzipEnabled="true" blockWhenQueueFull="true"
                 maxRetry="5" baseBackoffMillis="200">
      <PatternLayout pattern="%d{ISO8601} %level %logger - %msg%n"/>
    </OssAppender>
  </Appenders>

  <Loggers>
    <Root level="info">
      <AppenderRef ref="oss"/>
    </Root>
  </Loggers>
</Configuration>
```

或以 System Properties/环境变量方式填充敏感参数：`-DALIYUN_AK=xxx -DALIYUN_SK=yyy`。

> 插件发现：本库内置 `log4j2.component.properties` 指定包扫描路径，无需注解处理器。

## 参数说明

- `endpoint`：OSS Endpoint，例如 `https://oss-cn-hangzhou.aliyuncs.com`
- `accessKeyId` / `accessKeySecret`：访问凭证（建议使用 RAM 临时凭证/STS）
- `bucket`：目标 Bucket 名称
- `objectPrefix`：对象前缀，默认 `logs/`
- `queueSize`：内存队列容量（条）
- `maxBatchMessages`：单次批量最大条数
- `maxBatchBytes`：单次批量最大字节数
- `flushIntervalMillis`：批量最长滞留时间（毫秒）
- `gzipEnabled`：是否启用 GZIP 压缩
- `blockWhenQueueFull`：队列满时是否阻塞等待（否则丢弃）
- `maxRetry`：上传重试次数
- `baseBackoffMillis`：指数退避起始毫秒

## Cloudflare Pages/GitHub 导入

该库为纯 Java SDK，推荐以 Maven Central 使用。若需在 Cloudflare Pages 文档站或示例站展示，请将本仓库导入 GitHub 并在 Pages 绑定构建静态站（例如使用 Docusaurus/SvelteKit 文档）。

## 许可

Apache-2.0
