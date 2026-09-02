import org.gradle.api.tasks.PathSensitivity
import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget

plugins {
    kotlin("multiplatform") version "2.4.0"
    `maven-publish`
    signing
}

// com.netonstream 是本组织在 Maven Central 上已验证的 namespace。
// hyper4k 是独立库（不是 neton-* 框架模块），所以不带 neton- 前缀，也走自己的版本线。
group = "com.netonstream"
version = "0.1.1"

repositories {
    mavenCentral()
}

// K/Native target -> Rust target triple 映射
val rustTriples = mapOf(
    "macosArm64" to "aarch64-apple-darwin",
    "macosX64" to "x86_64-apple-darwin",
    "linuxX64" to "x86_64-unknown-linux-gnu",
    "linuxArm64" to "aarch64-unknown-linux-gnu",
    // Kotlin/Native 的 mingwX64 用 GNU ABI，所以是 -gnu 而不是 -msvc。
    // 交叉编译需要 mingw-w64（macOS: brew install mingw-w64），因为 TLS 后端
    // aws-lc-rs 含 C 代码；Rust 侧本身不需要目标平台 linker（staticlib 是纯归档）。
    "mingwX64" to "x86_64-pc-windows-gnu",
)

// Windows 交叉编译：C 代码必须用 K/N 链接时用的那套头文件来编译。
// Homebrew 的 mingw-w64 14 把 nanosleep 拆成 nanosleep32/64 内联转发，而 K/N 自带的
// msys2 sysroot 较老、只导出裸 nanosleep——用前者的头文件编译 aws-lc 的 C 代码，
// 链接时就会出现 undefined symbol: nanosleep64。所以把编译器指向 K/N 的 sysroot 头文件。
val konanMingwSysroot: File? by lazy {
    val deps = File(System.getProperty("user.home"), ".konan/dependencies")
    deps.listFiles()
        ?.filter { it.isDirectory && it.name.startsWith("msys2-mingw-w64-x86_64") }
        ?.map { File(it, "x86_64-w64-mingw32") }
        ?.firstOrNull { File(it, "include/pthread_time.h").isFile }
}

// Rust crate 目录（Kotlin 项目根下的 lib/ 子目录）
val crateDir = file("$projectDir/lib")
val headerDir = file("$crateDir/include")

kotlin {
    applyDefaultHierarchyTemplate()
    macosArm64()
    macosX64()
    linuxX64()
    linuxArm64()
    mingwX64()

    sourceSets {
        commonMain.dependencies {
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
        }
    }

    targets.withType<KotlinNativeTarget>().configureEach {
        val targetName = name
        val triple = rustTriples[targetName] ?: return@configureEach
        val libDir = file("$crateDir/target/$triple/release")

        val interop = compilations.getByName("main").cinterops.create("hyper4k") {
            defFile(project.file("src/nativeInterop/cinterop/hyper4k.def"))
            includeDirs(headerDir)
            // 静态库目录按 target 注入；libhyper4k.a 由 cargo 产出
            extraOpts("-libraryPath", libDir.absolutePath)
        }

        tasks.named(interop.interopProcessingTaskName).configure {
            dependsOn("cargoBuild${targetName.replaceFirstChar { it.uppercase() }}")
            // libhyper4k.a 是通过 -libraryPath 传进去的，Gradle 看不见它。不声明成输入的话，
            // cargo 重建了库、cinterop 仍然 UP-TO-DATE，klib 里会留着上一次的旧静态库。
            inputs.file(File(libDir, "libhyper4k.a")).withPathSensitivity(PathSensitivity.NONE)
        }

        // 链接 hyper4k 需要的系统库（Rust std / tokio 依赖）
        binaries.all {
            when (konanTarget.family) {
                org.jetbrains.kotlin.konan.target.Family.OSX -> linkerOpts("-framework", "Security", "-framework", "CoreFoundation")
                org.jetbrains.kotlin.konan.target.Family.LINUX -> linkerOpts("-lpthread", "-ldl", "-lm")
                org.jetbrains.kotlin.konan.target.Family.MINGW -> linkerOpts("-lws2_32", "-luserenv", "-lntdll", "-lbcrypt")
                else -> {}
            }
        }
    }

}

// --- 便捷任务：构建 Rust crate（每个 triple 一个 libhyper4k.a） ---
// 需要本机装好 rust 工具链与对应 target：`rustup target add <triple>`
rustTriples.forEach { (ktTarget, triple) ->
    tasks.register<Exec>("cargoBuild${ktTarget.replaceFirstChar { it.uppercase() }}") {
        group = "hyper4k"
        description = "cargo rustc --release --target $triple --crate-type staticlib"
        workingDir = crateDir
        // 只建 staticlib。Kotlin/Native 链接的是 libhyper4k.a，而 Cargo.toml 里另一个
        // crate-type（cdylib）只服务本地开发/JVM。交叉编译时 .a 是纯归档、不经 linker，
        // 而 .so 要用目标平台的 linker——在 macOS 上 `cargo build` 会因为 ld 不认
        // --version-script 之类的 GNU 选项而失败，卡住的是我们根本不用的那个产物。
        commandLine("cargo", "rustc", "--release", "--target", triple, "--crate-type", "staticlib")

        if (triple == "x86_64-pc-windows-gnu") {
            // 需要 mingw-w64 交叉工具链（macOS: brew install mingw-w64）来编译 aws-lc 的 C 代码，
            // 但头文件用 K/N 的 sysroot，保证编译期与链接期的 libc/pthread ABI 一致。
            environment("CC_x86_64_pc_windows_gnu", "x86_64-w64-mingw32-gcc")
            environment("AR_x86_64_pc_windows_gnu", "x86_64-w64-mingw32-ar")
            konanMingwSysroot?.let {
                environment("CFLAGS_x86_64_pc_windows_gnu", "-isystem ${File(it, "include").absolutePath}")
            }
        }
    }
}

// ---------- Maven Central 发布 ----------
//
// 四个 Rust target 的产物都是静态库（.a 是纯归档，不经目标平台 linker），
// 所以一台 macOS 机器就能构建并发布全部平台，不需要 Linux 机器。
//
// 发布走 Central Portal 的 bundle 上传，不用 OSSRH staging 端点：后者按客户端 IP
// 划分隐式 staging 仓库，长时间上传中途出口 IP 变化会把一次发布劈成两半。
//   ./gradlew publishAllPublicationsToStagingLocalRepository
// 然后按 RELEASING.md 打包上传。
publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set("hyper4k")
            description.set("HTTP/1.1 and HTTP/2 client and server for Kotlin/Native, backed by hyper.")
            url.set("https://github.com/netonframework/hyper4k")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    id.set("zoujiaqing")
                    name.set("zoujiaqing")
                    email.set("zoujiaqing@gmail.com")
                }
            }
            scm {
                url.set("https://github.com/netonframework/hyper4k")
                connection.set("scm:git:https://github.com/netonframework/hyper4k.git")
                developerConnection.set("scm:git:ssh://git@github.com/netonframework/hyper4k.git")
            }
        }
    }

    repositories {
        // 本地文件仓库：产出可直接打成 Central Portal bundle 的目录树（含签名与校验和）
        maven {
            name = "stagingLocal"
            url = uri(layout.buildDirectory.dir("staging-repo"))
        }
    }
}

// Central 强制签名。优先内存 key（CI / 无 keyring 的机器），其次本地 keyring。
signing {
    val inMemoryKey = findProperty("signingInMemoryKey") as String?
    if (!inMemoryKey.isNullOrBlank()) {
        useInMemoryPgpKeys(inMemoryKey, findProperty("signingInMemoryKeyPassword") as String? ?: "")
        sign(publishing.publications)
    } else if (hasProperty("signing.keyId")) {
        sign(publishing.publications)
    }
}

// 不加 javadoc jar：klib 打包的 KMP publication，Central 校验不要求它
// （neton 全部模块也是这样发布并通过校验的）。多个 publication 共用一个 javadoc jar
// 还会让多个 Sign 任务写同一个 .asc，Gradle 直接报输出冲突。
