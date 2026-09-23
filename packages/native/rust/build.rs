//! Embeds the library's source and build identity and, on macOS, links the
//! compiler-rt archive that the pinned libwebrtc needs.
//!
//! The identity is a JSON object that `scripts/stage.mjs` finds in the built
//! library and checks against the checked-out sources before staging it. Its
//! `sourceSha256` covers the same files, hashed the same way, as
//! `stage.mjs --source-hash` and the pack check in `scripts/pack.ts`.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The C ABI version. A library test checks it against `abi::ABI_VERSION`.
const ABI_VERSION: u32 = 3;

/// The source identity's inputs outside `src/`, which it covers entirely.
const SOURCE_FILES: [&str; 5] = [
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    ".cargo/config.toml",
    "include/reactor_effect_native.h",
];

/// Environment that changes the compiled library, recorded when set.
const BUILD_ENVIRONMENT: [&str; 6] = [
    "CC",
    "CXX",
    "CARGO_ENCODED_RUSTFLAGS",
    "CFLAGS",
    "CXXFLAGS",
    "MACOSX_DEPLOYMENT_TARGET",
];

fn main() {
    let root =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets the manifest dir"));
    let identity = build_identity(&root);
    println!("cargo::rustc-env=REACTOR_EFFECT_BUILD_IDENTITY={identity}");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        link_compiler_rt();
    }
}

/// The identity JSON: the source hash, target and profile, the toolchain's
/// versions and the build environment.
fn build_identity(root: &Path) -> String {
    let cc = env::var("CC").unwrap_or_else(|_| "clang".to_owned());
    let cxx = env::var("CXX").unwrap_or_else(|_| "clang++".to_owned());
    let rustc = env::var("RUSTC").expect("Cargo sets RUSTC");
    // Each value is already JSON.
    let mut fields = vec![
        ("schemaVersion", "1".to_owned()),
        ("abiVersion", ABI_VERSION.to_string()),
        ("sourceSha256", json_string(&source_sha256(root))),
        ("target", json_string(&cargo_env("TARGET"))),
        ("profile", json_string(&cargo_env("PROFILE"))),
        ("rustc", json_string(&tool_version(&rustc))),
        ("cc", json_string(&tool_version(&cc))),
        ("cxx", json_string(&tool_version(&cxx))),
    ];
    for key in BUILD_ENVIRONMENT {
        println!("cargo::rerun-if-env-changed={key}");
        if let Ok(value) = env::var(key) {
            fields.push((key, json_string(&value)));
        }
    }
    let fields: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("{}:{value}", json_string(key)))
        .collect();
    format!("{{{}}}", fields.join(","))
}

fn cargo_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Cargo sets {key}"))
}

/// SHA-256 over each source input, as `path NUL contents NUL` in path order.
fn source_sha256(root: &Path) -> String {
    let mut files = SOURCE_FILES.map(str::to_owned).to_vec();
    files.extend(files_under(root, &root.join("src")));
    files.sort();
    // The directory as well, so a new source file reruns this script.
    println!("cargo::rerun-if-changed=src");
    let mut input = Vec::new();
    for file in files {
        println!("cargo::rerun-if-changed={file}");
        let contents = fs::read(root.join(&file))
            .unwrap_or_else(|error| panic!("read native build input {file}: {error}"));
        input.extend_from_slice(file.as_bytes());
        input.push(0);
        input.extend_from_slice(&contents);
        input.push(0);
    }
    sha256_hex(&input)
}

/// Every file under `directory`, as a `/`-separated path relative to `root`.
fn files_under(root: &Path, directory: &Path) -> Vec<String> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).expect("the native source directory exists") {
        let path = entry.expect("a readable native source entry").path();
        if path.is_dir() {
            files.extend(files_under(root, &path));
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("sources are under the crate root");
            let relative = relative.to_str().expect("source paths are UTF-8");
            files.push(relative.replace('\\', "/"));
        }
    }
    files
}

/// SHA-256 as hex, from the platform's tool rather than a build dependency.
fn sha256_hex(input: &[u8]) -> String {
    let mut command = if cfg!(target_os = "macos") {
        let mut shasum = Command::new("shasum");
        shasum.args(["-a", "256"]);
        shasum
    } else {
        Command::new("sha256sum")
    };
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("a SHA-256 tool is required to identify native build inputs");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(input)
        .expect("hash native build inputs");
    let output = child.wait_with_output().expect("join native source hash");
    assert!(output.status.success(), "native source hash failed");
    let text = String::from_utf8(output.stdout).expect("the hash tool prints UTF-8");
    let digest = text
        .split_whitespace()
        .next()
        .expect("the hash tool prints a digest");
    assert!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "unexpected SHA-256 digest {digest:?}"
    );
    digest.to_owned()
}

/// The first line of `program --version`.
fn tool_version(program: &str) -> String {
    let output = Command::new(program)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| panic!("could not inspect native build tool {program}: {error}"));
    assert!(
        output.status.success(),
        "native build tool {program} failed"
    );
    let text = String::from_utf8(output.stdout).expect("build tool versions are UTF-8");
    text.lines().next().unwrap_or_default().to_owned()
}

/// `value` as a JSON string literal.
fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                write!(out, "\\u{:04x}", u32::from(ch)).expect("writing to a String cannot fail");
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// The pinned Reactor libwebrtc archive contains `ScreenCaptureKit` objects
/// produced by Clang. They reference compiler-rt's deployment-version helper,
/// which rustc's macOS link line does not add on its own.
fn link_compiler_rt() {
    let cc = env::var("CC").unwrap_or_else(|_| "clang".to_owned());
    let output = Command::new(cc)
        .arg("-print-resource-dir")
        .output()
        .expect("clang is required to link the Reactor libwebrtc archive on macOS");
    assert!(output.status.success(), "clang -print-resource-dir failed");
    let resource = String::from_utf8(output.stdout).expect("clang's resource path is UTF-8");
    let directory = PathBuf::from(resource.trim()).join("lib/darwin");
    let runtime = directory.join("libclang_rt.osx.a");
    assert!(
        runtime.is_file(),
        "missing macOS compiler-rt archive at {}",
        runtime.display()
    );
    println!("cargo::rustc-link-search=native={}", directory.display());
    println!("cargo::rustc-link-lib=static=clang_rt.osx");
    // Cargo passes rustc-link-lib only to the library target, and the far-peer
    // example cannot link this cdylib, so it names the archive itself.
    println!("cargo::rustc-link-arg-examples={}", runtime.display());
}
