# Releasing

All five targets are published from one macOS host. The Rust artifacts are static libraries
(`.a` is a plain archive and never goes through the target linker), so cross-compilation needs
no target linker and no Linux or Windows machine.

## Prerequisites

```
rustup target add aarch64-apple-darwin x86_64-apple-darwin \
                  aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu \
                  x86_64-pc-windows-gnu
brew install mingw-w64   # only for the Windows target
```

The Windows target uses the GNU ABI (`-gnu`, not `-msvc`) because that is what Kotlin/Native's
mingwX64 links against. mingw-w64 is needed to compile the C sources in `aws-lc-sys`, the TLS
backend; the build compiles them against Kotlin/Native's own msys2 sysroot headers so the
compile-time and link-time libc agree. Building against Homebrew's newer headers instead yields
`undefined symbol: nanosleep64` at link time.

`~/.gradle/gradle.properties`:

```
sonatypeUsername=<Central Portal user token>
sonatypePassword=<Central Portal user token password>
signingInMemoryKey=<base64 PGP secret key>
signingInMemoryKeyPassword=<key passphrase>
```

The signing public key must be on a keyserver (`keyserver.ubuntu.com` or `keys.openpgp.org`).

## Publish

Use the Central Portal bundle upload, not the OSSRH staging endpoint: that API keys its implicit
staging repository by client IP, so an exit-IP change during a long upload silently splits the
release across two repositories. A bundle upload is a single request and cannot split.

```bash
# 1. Build, sign and lay out every publication
./gradlew publishAllPublicationsToStagingLocalRepository

# 2. Bundle it (repository-level metadata is not part of a bundle)
cd build/staging-repo
find com -name 'maven-metadata.xml*' -delete
zip -qr ../hyper4k-<version>.zip com
cd -

# 3. Upload with USER_MANAGED so validation can be inspected before anything goes live
TOKEN=$(printf '%s:%s' "$SONATYPE_USER" "$SONATYPE_PASS" | base64)
curl -X POST -H "Authorization: Bearer $TOKEN" \
     -F "bundle=@build/hyper4k-<version>.zip" \
     "https://central.sonatype.com/api/v1/publisher/upload?name=hyper4k-<version>&publishingType=USER_MANAGED"

# 4. Poll until VALIDATED, then publish
curl -X POST -H "Authorization: Bearer $TOKEN" "https://central.sonatype.com/api/v1/publisher/status?id=<deployment id>"
curl -X POST -H "Authorization: Bearer $TOKEN" "https://central.sonatype.com/api/v1/publisher/deployment/<deployment id>"

# 5. Confirm every coordinate (root plus one per target)
curl -H "Authorization: Bearer $TOKEN" \
     "https://central.sonatype.com/api/v1/publisher/published?namespace=com.netonstream&name=hyper4k&version=<version>"
```

Publishing to Central is irreversible. Check the validation result before step 4.

## Verify

Tag the release, then build a throwaway project that resolves only from `mavenCentral()` and
depends on `com.netonstream:hyper4k:<version>`. Source-path builds (`includeBuild`) do not prove
that the published artifacts work.
