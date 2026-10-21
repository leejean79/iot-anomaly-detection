// ============================================================================
// m5b/OptWinBench.java
// V-M5b-5（每个元素的处理时延）的微基准：在元素流文件上逐台运行检测器与持续性核对，重复六遍，打印每个元素的平均耗时与
// 最长一次（第一遍含即时编译预热，不计最长）。只计检测器与核对本身，不含 Flink 的状态访问与序列化（作业用堆内状态后端）。
// Micro-benchmark of OptWinDetector + DriftGate per element (V-M5b-5).
//
// ---------------------------- 脚本交付五要素 -------------------------------
// 1. 执行环境 / Environment: 任意装有 JDK 11 及以上的机器，仓库根目录（单文件源码直接运行，无需编译）。
// 2. 调用命令 / Invocation:
//      java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar m5b/OptWinBench.java docs/m5b/streams_10min.csv
// 3. 前置条件 / Preconditions: 已 mvn package；元素流由 m5b/prepare_streams.py 生成（含 device 与 main_score 列）。
// 4. 期望产出 / Expected output: 六行「rep i: N elements, mean x us/element, worst y us」。
// 5. 失败兜底 / Failure fallback: 找不到类 → 先 mvn package；列名不符 → 检查元素流表头。
// ============================================================================
import com.leejean.m5.*;
import java.nio.file.*; import java.util.*;
public class OptWinBench {
  public static void main(String[] a) throws Exception {
    List<String> lines = Files.readAllLines(Paths.get(a[0]));
    String[] h = lines.get(0).split(","); int iv = Arrays.asList(h).indexOf("main_score"), id = 0;
    Map<String,List<Double>> m = new LinkedHashMap<>();
    for (String l : lines.subList(1, lines.size())) { String[] f = l.split(","); m.computeIfAbsent(f[id], z -> new ArrayList<>()).add(Double.parseDouble(f[iv])); }
    OptWinTables t = OptWinTables.build(0.99, 0.5, 2016);
    for (int rep = 0; rep < 6; rep++) {
      long n = 0, t0 = System.nanoTime(); long worst = 0;
      for (Map.Entry<String,List<Double>> e : m.entrySet()) {
        OptWinDetector d = new OptWinDetector(2016); DriftGate g = new DriftGate(36, 0.5);
        for (double x : e.getValue()) { long s = System.nanoTime(); g.onElement(e.getKey(), n, x, d.update(x, t)); long dt = System.nanoTime() - s; if (dt > worst && rep > 0) worst = dt; n++; }
      }
      long el = System.nanoTime() - t0;
      System.out.printf("rep %d: %d elements, mean %.2f us/element, worst %.1f us%n", rep, n, el / 1e3 / n, worst / 1e3);
    }
  }
}
