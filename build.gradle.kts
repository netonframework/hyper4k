import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget

plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

repositories {
    mavenCentral()
}

// K/Native target -> Rust target triple 映射
val rustTriples = mapOf(
    "macosArm64" to "aarch64-apple-darwin",
    "macosX64" to "x86_64-apple-darwin",
    "linuxX64" to "x86_64-unknown-linux-gnu",
    "linuxArm64" to "aarch64-unknown-linux-gnu",
    "mingwX64" to "x86_64-pc-windows-gnu",
)

// Rust crate 目录（Kotlin 项目根下的 lib/ 子目录）
val crateDir = file("$projectDir/lib")
val headerDir = file("$crateDir/include")

kotlin {
    macosArm64()
    macosX64()
    linuxX64()
    linuxArm64()
    mingwX64()

    targets.withType<KotlinNativeTarget>().configureEach {
        val triple = rustTriples[name] ?: return@configureEach
        val libDir = file("$crateDir/target/$triple/release")

        compilations.getByName("main").cinterops.create("hyper4k") {
            defFile(project.file("src/nativeInterop/cinterop/hyper4k.def"))
            includeDirs(headerDir)
            // 静态库目录按 target 注入；libhyper4k.a 由 cargo 产出
            extraOpts("-libraryPath", libDir.absolutePath)
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

    sourceSets {
        val commonMain by getting
        val nativeMain by creating { dependsOn(commonMain) }
        listOf("macosArm64Main", "macosX64Main", "linuxX64Main", "linuxArm64Main", "mingwX64Main")
            .forEach { getByName(it).dependsOn(nativeMain) }
    }
}

// --- 便捷任务：构建 Rust crate（每个 triple 一个 libhyper4k.a） ---
// 需要本机装好 rust 工具链与对应 target：`rustup target add <triple>`
rustTriples.forEach { (ktTarget, triple) ->
    tasks.register<Exec>("cargoBuild${ktTarget.replaceFirstChar { it.uppercase() }}") {
        group = "hyper4k"
        description = "cargo build --release --target $triple"
        workingDir = crateDir
        // 跨平台交叉编译建议用 cargo-zigbuild：
        //   commandLine("cargo", "zigbuild", "--release", "--target", triple)
        commandLine("cargo", "build", "--release", "--target", triple)
    }
}
