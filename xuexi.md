zambezi发版导致下游moras服务抖动问题
一. 现象
zambezi-api-coupon这个hostgroup组，每次发版的时候，gc count会比日常高很多，新服务注册后，上游rpc调用过来时经常timeout。
moras-api是zambezi-api的主要上游，rpc异常跟hostgroup的发布周期的曲线基本一致。
六、JIT C2编译优化伴随着JVM长时间停顿
大佬们，目前我们有一个初步方向：
CPU抖动是因为JIT对代码做C2编译优化引起的。

晚上我发布了一台机器，https://monitor.pdd.net/server/transaction-new?Type=DubboService&end_time=1773317321000&ip=10.215.196.36&service=zambezi-api&serviceIndexPath=transaction-name&time_range=30m

该机器于20:06分出现CPU飙升，伴随着moras-api的超时报错，我下载了JFR文件，显示20：06分，有一个2.542秒的停顿
具体查看线程信息，我们发现同时间点，JIT使用C2优化对一个方法优化了4s

微服务侧有一个和我们现象类似的案例：https://note.pdd.net/doc/689938795500802048?root=202754979624304640
该机器的GC日志：https://ofs.pdd.net/bianque-api/10.215.196.36/20260312-201934-gc-log/gc.log.0
该机器的JFR文件：https://ofs.pdd.net/kernel-stat/command/fc0d3fa6-abb2-45a8-b81f-44c695ad4cd8/run.zip

6.1 ❌禁止对目标方法编译优化
-XX:CompileCommand=exclude,com/pinduoduo/service/aal/proto/Feature,"<init>"
结论：添加该参数后，CPU飙升，改方案不可行
原因：虽然AI提到该命令是禁止JIT对该方法进行C2优化，可以进行C1优化，但是Oracle官方文档显示该命令是禁止对该方法进行编译，并未提到是C1还是C2优化，结合线上实操，该方法不可行
6.2 ❌禁止编译器进行C2优化
尝试直接禁用C2编译优化：-XX:TieredStopAtLevel=1
结论：添加该参数后，CPU飙升，改方案不可行
6.3 ❌优化safepoint的STW时间
-XX:CICompilerCount=2：限制JIT编译优化时的线程数量
-XX:LoopUnrollLimit=0 ：限制循环展开
原理：JIT编译优化时是可以和应用线程同时运行的，只不过在初始阶段会触发safepoints，GC log显示：
[2026-03-12T20:06:04.015+0800][info ][safepoint      ] Safepoint "Cleanup", Time since last: 2000163220 ns, Reaching safepoint: 2541547347 ns, At safepoint: 359683 ns, Total: 2541907030 ns
到达暂停点耗时2.54秒，说明JVM在发出暂停信号后，有的线程一直在执行逻辑没有到达安全点，很可能陷入到一个大的循环无法跳出来。
效果：
CPU pressure上涨减小
机器1：链接

moras超时减少：
结论：并没有完全解决抖动问题，而且样本比较少，后续再继续研究一下

6.4 ✅C2编译预热
6.4.1 原理
JIT的编译优化有一套逻辑
  热度=方法调用计数+K×回边计数
- 方法调用计数器：每次进入方法 +1。
- 回边计数器：每次方法内部循环跳转 +1。这个值的权重 K 通常很大（比如 30-50），因为循环代码更耗时，更值得优化
所以当一个方法被调用的次数越多，他内部的循环被执行的次数越多，他越可能被JIT优化。
如果我们在启动的时候调用该方法足够的次数，让其内部循环执行足够的次数，那么它很可能就在启动阶段就被编译优化了，而不是在线上流量进来的时候
6.4.2 方案
  构造Feature数据，让其执行足够多的循环，被调用的足够多。
  为什么不直接加大当前线上预热的调用次数？
    1.当前预热6w次耗时2分钟，加大预热次数会加长机器的启动时间，对发版不友好
    2.当前预热调用了下游GRPC，加大预热会增加下游压力
    3.预热的流量可能不会覆盖该方法的所有逻辑分支，导致C2进行剪枝，上线后被剪枝的流量请求过来的时候，触发二次编译
6.4.3 本地测试
  结果：成功的触发了该方法的C2编译，测试方法10s跑完
<nmethod compile_id='364' compiler='c2' level='4' entry='0x0000000113d441c0' size='24184' address='0x0000000113d43b10' relocation_offset='344' insts_offset='1712' stub_offset='15536' scopes_data_offset='18152' scopes_pcs_offset='21712' dependencies_offset='23584' handler_table_offset='23608' nul_chk_table_offset='24064' oops_offset='17680' metadata_offset='17744' method='com.pinduoduo.service.aal.proto.Feature &lt;init&gt; (Lcom/google/protobuf/CodedInputStream;Lcom/google/protobuf/ExtensionRegistryLite;)V' bytes='927' count='2238' backedge_count='3603607' iicount='2238' stamp='0.144'/>
  预热代码
@Slf4j
@Component
public class C2CompilationWarmUpService {
    @Autowired
    private ZambeziWarmUpConfig warmUpConfig;

    @PostConstruct
    public void warmUp() {
        featureCompilationWarmUp();
    }

    private void featureCompilationWarmUp() {
        Transaction warmUpTran = Cat.newTransaction("C2CompilationWarmUp", "feature");
        try {
            //构造数据
            byte[] featureData = generateComplexFeature(warmUpConfig.getFeatureC2WarmUpByteStreamKB() * 1024);
            Date start = new Date();
            log.info("feature warmup start: {}", start);
            Feature feature = Feature.newBuilder().build();
            //触发反序列化逻辑, 运行100w次，给JIT充足预热
            int executeTimes = warmUpConfig.getFeatureC2WarmUpExecuteTimes();
            for (int i = 0; i < executeTimes; i++) {
                ByteArrayInputStream byteInputStream = new ByteArrayInputStream(featureData);
                feature = ProtoLiteUtils.marshaller(feature).parse(byteInputStream);
            }
            Date end = new Date();
            String featureName = Objects.isNull(feature) ? "null" : feature.getName();
            log.info("feature warmup end:{} featureName{} cost:{}ms", end, featureName, end.getTime() - start.getTime());
            warmUpTran.setStatus(Message.SUCCESS);
        } catch (Exception e) {
            log.error("feature warmup error", e);
            warmUpTran.setStatus("-1");
        } finally {
            warmUpTran.complete();
        }
    }

    //feature类的GRPC二进制数据流构造
    private byte[] generateComplexFeature(int targetSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writeVarint(baos, 8); // Tag 8
        writeVarint(baos, 12345);


        writeVarint(baos, 16); // Tag 16
        writeVarint(baos, 200L);

        writeVarint(baos, 26); // Tag 26
        byte[] dummyBytes = "test_bytes_data".getBytes();
        writeVarint(baos, dummyBytes.length);
        baos.write(dummyBytes);


        writeVarint(baos, 37); // Tag 37 (Fixed32/Float)
        writeFloatLE(baos, 3.14f);

        // Field 5: Int64Array -> case 42
        writeVarint(baos, 42); // Tag 42
        // 写入一个空的 Int64Array 消息 (长度为 0)
        writeVarint(baos, 0);

        // Field 6: BytesArray -> case 50
        writeVarint(baos, 50); // Tag 50
        // 写入一个空的 BytesArray 消息 (长度为 0)
        writeVarint(baos, 0);

        // Field 7: FloatArray -> case 58
        writeVarint(baos, 58); // Tag 58
        // 写入一个空的 FloatArray 消息 (长度为 0)
        writeVarint(baos, 0);

        String name = "feature_benchmark_test";
        writeVarint(baos, 66); // Tag 66
        writeVarint(baos, name.length());
        baos.write(name.getBytes());

        // 3. 循环写入 Packed 数据直到达到目标大小
        // 我们主要使用 packed float 和 packed int64 来填充体积
        while (baos.size() < targetSize) {
            ByteArrayOutputStream tempStream = new ByteArrayOutputStream();
            int count = 200;
            for (int i = 0; i < count; i++) {
                writeVarint(tempStream, RandomUtils.nextLong());
            }
            byte[] payload = tempStream.toByteArray();
            writeVarint(baos, 18);
            writeVarint(baos, payload.length);
            baos.write(payload);


            writeVarint(baos, 34);
            int payloadSize = count * 4;
            writeVarint(baos, payloadSize);
            for (int i = 0; i < count; i++) {
                writeFloatLE(baos, RandomUtils.nextFloat());
            }

        }

        return baos.toByteArray();
    }

    private void writeVarint(ByteArrayOutputStream out, long value) throws IOException {
        int i = 10000;
        while (i > 0) {
            i = i - 1;
            if ((value & ~0x7FL) == 0) {
                out.write((int) value);
                return;
            } else {
                out.write(((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    private void writeFloatLE(ByteArrayOutputStream out, float value) {
        int bits = Float.floatToIntBits(value);
        out.write(bits & 0xFF);
        out.write((bits >> 8) & 0xFF);
        out.write((bits >> 16) & 0xFF);
        out.write((bits >> 24) & 0xFF);
    }
}
  测试环境：
  OpenJDK 64-Bit Server VM (17.0.18+8) for bsd-aarch64 JRE (17.0.18+8), built on Jan 20 2026 00:00:00 by &quot;admin&quot; with clang Apple LLVM 15.0.0 (clang-1500.1.0.2.5)
  VM参数：
  -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation -XX:LogFile=/Users/centbaby/Downloads/jit_555.log -Dfile.encoding=UTF-8 -Duser.country=GB -Duser.language=en -Duser.variant 
  JIT日志：
jit_555.log
6.4.4 为什么线上6w次的预热没触发该方法的C2编译优化
  这个问题我们只能做推理，根据上面的公式，其实内部循环次数比方法被调用次数占的权重更大，或许线上环境的预热数据循环执行的比较少，又或者整个项目的热点代码比较多，编译器优先处理了其他代码的编译

七、总结
# Java JIT 编译优化与 OSR 抖动问题深度剖析

## 1. 概述
Java 最初作为解释型语言执行，具备良好的跨平台特性，但解释执行的性能开销较大。为了兼顾启动速度与运行时峰值性能，JVM 引入了即时编译器。JIT 在应用运行期间持续监控代码执行状态，对频繁调用的“热点代码”进行动态编译优化，将其转化为高度优化的本地机器码，从而大幅提升系统吞吐量。然而，在特定的编译优化机制下，如果代码结构不合理，极易引发服务延迟抖动。

## 2. JIT 分层编译机制
现代 JVM（如 HotSpot）普遍采用分层编译策略，将优化过程划分为不同层级，以平衡编译开销与执行性能：

*   **C1 编译器（Client Compiler）：** 主要进行快速且轻量的优化，如方法内联、去虚拟化等。其目标是尽可能减少编译时间，快速提升代码执行效率。
*   **C2 编译器（Server Compiler）：** 执行最极致、最深度的优化。C2 会基于解释器和 C1 采集的运行时性能剖析数据，进行逃逸分析、循环展开、锁消除等激进优化，最终生成接近 C/C++ 级别的底层汇编机器码。

## 3. 栈上替换与 STW 机制
### 3.1 OSR (On-Stack Replacement)
JIT 的优化不仅针对方法调用入口，对于长时间运行的热点循环体，JVM 会采用**栈上替换（OSR）**技术。当一段正在解释执行的循环代码被 C2 编译优化完毕后，JVM 会在当前栈帧上，将解释执行状态无缝切换为执行 C2 生成的本地机器码状态。

### 3.2 安全点与 STW
执行 OSR 切换是一项极其敏感的系统级操作。为了保障内存状态和执行上下文的一致性，JVM 必须进入 **Stop-The-World (STW)** 阶段，暂停所有应用线程。
*   **抖动根因：** 在 STW 期间，JVM 无法处理任何外部请求，直接表现为服务接口的延迟毛刺或抖动。

## 4. 长耗时与高并发代码的替换陷阱
OSR 切换并非瞬间完成。当目标代码段满足以下特征时，将导致极其严重的 STW 停顿：

1.  **代码执行耗时较长：** 例如包含深层次的复杂循环。
2.  **并发执行线程众多：** 大量业务线程同时堆叠在该代码段内执行。

**机制解析：**
JVM 在进行 OSR 前，需要等待所有正在执行该段代码的线程到达**安全点**。如果代码执行时间长且并发量大，JVM 必须耗费大量时间等待这些线程逐一跑完当前代码逻辑并退出或到达安全点。在此漫长的等待期间，JVM 全局挂起，不仅无法处理新请求，还会引发显著的流量阻塞与超时。

## 5. 代码规模与 JIT 优化的关联
JIT 编译器天然偏好体积小、逻辑简单的代码块。这并非仅仅因为“小代码好编译”，其核心逻辑在于规避上述的 OSR 等待陷阱：

*   **短代码执行迅速：** 线程进出代码块的时间极短，同一时刻停留在该代码块内部的线程数量极少。
*   **降低安全点等待时间：** 当触发 OSR 时，JVM 几乎不需要等待线程执行完毕，可以瞬间完成栈上替换，将 STW 时间压缩至亚毫秒级，从而避免对线上服务造成可感知的抖动。

## 6. 基于剖析数据的分支剪枝与逆优化
### 6.1 激进优化与剪枝
C2 编译器是一种基于历史的推测性优化器。它高度依赖前期采集的性能剖析数据。对于条件分支（如 `if-else`），如果在历史采集中某分支从未被触发，C2 会进行**分支剪枝**，直接在生成的机器码中剔除该死代码路径，以减少指令缓存压力并提升执行效率。

### 6.2 逆优化与重编译
推测性优化存在失效风险。如果后续业务流量触发了被 C2 剪枝的“冷门分支”，JVM 会触发**逆优化**：
1.  抛弃当前 C2 编译的机器码，回退至解释执行模式或 C1 代码。
2.  重新采集包含新分支的执行数据。
3.  再次触发 C2 重新编译，生成包含新分支的机器码。

这一逆优化与重编译过程会带来巨大的性能开销，并可能再次引发 STW 和 OSR。

## 7. 工程实践建议：系统预热策略
为了避免线上环境因 JIT 编译引发性能抖动，必须在服务上线前进行充分的**系统预热**：

1.  **全分支覆盖：** 预热流量或预热脚本必须覆盖业务代码的所有核心逻辑分支，确保 C2 在首次编译时能采集到完整的执行路径，避免后续运行时发生“逆优化 -> 重编译”的震荡。
2.  **拆分长方法：** 对于包含复杂长循环或深层嵌套的方法，应进行重构拆分，将其分解为多个短小的方法。这不仅能加速 JIT 编译，更能有效规避高并发下的 OSR 等待陷阱。
3.  **压测驱动预热：** 在服务发布阶段，通过逐步递增的压测流量模拟真实生产负载，强制触发关键链路的热点代码编译，待 CPU 利用率和 RT 稳定后再接入真实流量。



