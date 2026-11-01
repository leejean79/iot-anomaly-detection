package com.leejean.m7;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在线算子状态的类加载器回归（2026-11-01 短重放无限重启的根因）：集群上作业 jar 的类只有用户代码类加载器看得见，Flink 把它设为
 * Kryo 的类加载器；状态的反序列化必须经它解析类。本测试用一个「子优先」的类加载器模拟用户代码类加载器（它自己加载 com.leejean
 * 下的类），检验 {@link M7InputAssembler} 的默认 Kryo 序列化器是 Flink 的 JavaSerializer，且读回的对象的类来自这个类加载器。
 * Kryo 自带的 JavaSerializer 会用系统类加载器解析，在集群上就是 ClassNotFoundException。
 * Regression for the state class-loader bug: deserialization must resolve classes through Kryo's (user-code) class loader.
 */
class M7StateClassLoaderTest {

    /** 子优先：com.leejean 下的类由自己加载，其余交给父加载器 / child-first for com.leejean classes. */
    private static final class ChildFirst extends URLClassLoader {
        ChildFirst(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null && name.startsWith("com.leejean.")) {
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        c = null;
                    }
                }
                if (c == null) {
                    c = getParent().loadClass(name);
                }
                if (resolve) {
                    resolveClass(c);
                }
                return c;
            }
        }
    }

    @Test
    @DisplayName("状态经 Kryo 的类加载器读回（Flink 的 JavaSerializer）/ state resolves classes via Kryo's class loader")
    @SuppressWarnings({"unchecked", "rawtypes"})
    void stateUsesKryoClassLoader() throws Exception {
        KryoSerializer<M7InputAssembler> ks = new KryoSerializer<>(M7InputAssembler.class, new ExecutionConfig());
        Kryo kryo = ks.getKryo();
        Serializer ser = kryo.getDefaultSerializer(M7InputAssembler.class);
        assertTrue(ser instanceof JavaSerializer, "须为 Flink 的 JavaSerializer，实为 " + ser.getClass().getName());

        M7InputAssembler a = new M7InputAssembler(M7OfflineRunner.params(M7InputAssemblerTest.defaultArgs()));
        M7Input in = new M7Input();
        in.kind = M7Input.ROUND;
        in.device = "A";
        in.ts = FleetDecisionFunction.dayStart("2022-03-10");
        in.tv = new double[]{20.0, 500.0, 3.0};
        a.accept(in);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (Output out = new Output(bos)) {
            ser.write(kryo, out, a);
        }

        URL classes = M7InputAssembler.class.getProtectionDomain().getCodeSource().getLocation();
        try (ChildFirst user = new ChildFirst(new URL[]{classes}, M7StateClassLoaderTest.class.getClassLoader())) {
            kryo.reset();                                     // 清掉写入时缓存的流 / drop the cached output stream
            kryo.setClassLoader(user);
            Object back;
            try (Input input = new Input(bos.toByteArray())) {
                back = ser.read(kryo, input, M7InputAssembler.class);
            }
            assertSame(user, back.getClass().getClassLoader(), "读回的类须来自 Kryo 的类加载器");
            assertEquals(M7InputAssembler.class.getName(), back.getClass().getName());
            // 堆状态在异步检查点期间按写时复制取值，经 KryoSerializer.copy（2026-11-01 第一次失败的位置）/ copy-on-write path
            kryo.reset();
            Object copy = ks.copy(a);
            assertSame(user, copy.getClass().getClassLoader(), "复制出的对象的类须来自 Kryo 的类加载器");
        }
    }
}
