//! Embeds the library's source and build identity and, on macOS, links the
//! compiler-rt archive that the pinned libwebrtc needs.
//!
//! The identity is a JSON object that `scripts/stage.mjs` finds in the built
//! library and checks against the checked-out sources before staging it. Its
//! `sourceSha256` covers the same files, hashed the same way, as
//! `stage.mjs --source-hash` and the pack check in `scripts/pack.ts`. Its
//! `webrtcPrebuilt` names the Reactor libwebrtc prebuilt that
//! `reactor-webrtc-sys` links, which staging checks against the shipped SBOM.

use std::env;
use std::error::Error;
use std::fmt::{self, Write as _};
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// A failed build step, which Cargo reports with the script's output.
type BuildResult<T> = Result<T, Box<dyn Error>>;

/// The C ABI version. A library test checks it against `abi::ABI_VERSION`.
const ABI_VERSION: u32 = 4;

/// The source identity's inputs outside `src/`, which it covers entirely.
const SOURCE_FILES: [&str; 5] = [
    "Cargo.toml",
    "Cargo.lock",
    "build.rs",
    ".cargo/config.toml",
    "include/reactor_effect_native.h",
];

/// Overrides that make `reactor-webrtc-sys` link something other than its
/// tagged prebuilt; with either set, the identity names no prebuilt.
const WEBRTC_OVERRIDES: [&str; 2] = ["REACTOR_WEBRTC_LIB_DIR", "REACTOR_WEBRTC_PREBUILT_URL"];

/// Environment that changes the compiled library, recorded when set.
const BUILD_ENVIRONMENT: [&str; 6] = [
    "CC",
    "CXX",
    "CARGO_ENCODED_RUSTFLAGS",
    "CFLAGS",
    "CXXFLAGS",
    "MACOSX_DEPLOYMENT_TARGET",
];

fn main() -> BuildResult<()> {
    let root = env::var_os("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .ok_or("Cargo sets CARGO_MANIFEST_DIR")?;
    let identity = build_identity(&root)?;
    println!("cargo::rustc-env=REACTOR_EFFECT_BUILD_IDENTITY={identity}");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        link_compiler_rt()?;
    }
    Ok(())
}

/// The identity JSON: the source hash, target and profile, the toolchain's
/// versions and the build environment.
fn build_identity(root: &Path) -> BuildResult<String> {
    let cc = env::var("CC").unwrap_or_else(|_| "clang".to_owned());
    let cxx = env::var("CXX").unwrap_or_else(|_| "clang++".to_owned());
    let rustc = cargo_env("RUSTC")?;
    // Each value is already JSON.
    let mut fields = vec![
        ("schemaVersion", "1".to_owned()),
        ("abiVersion", ABI_VERSION.to_string()),
        ("sourceSha256", json_string(&source_sha256(root)?)),
        ("target", json_string(&cargo_env("TARGET")?)),
        ("profile", json_string(&cargo_env("PROFILE")?)),
        ("rustc", json_string(&tool_version(&rustc)?)),
        ("cc", json_string(&tool_version(&cc)?)),
        ("cxx", json_string(&tool_version(&cxx)?)),
        (
            "webrtcPrebuilt",
            webrtc_prebuilt(root)?.map_or_else(|| "null".to_owned(), |tag| json_string(&tag)),
        ),
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
    Ok(format!("{{{}}}", fields.join(",")))
}

/// The prebuilt tag `reactor-webrtc-sys` downloads, derived as it derives it:
/// from `WEBRTC_VERSION` at the root of the pinned `reactor-webrtc` checkout.
/// `None` when an override links a local or custom archive instead.
fn webrtc_prebuilt(root: &Path) -> BuildResult<Option<String>> {
    for key in WEBRTC_OVERRIDES {
        println!("cargo::rerun-if-env-changed={key}");
        if env::var_os(key).is_some() {
            return Ok(None);
        }
    }
    let output = Command::new(cargo_env("CARGO")?)
        .args(["metadata", "--format-version", "1", "--offline", "--locked"])
        .arg("--manifest-path")
        .arg(root.join("Cargo.toml"))
        .output()
        .map_err(|error| format!("cargo metadata could not run: {error}"))?;
    if !output.status.success() {
        return Err(format!("cargo metadata failed: {}", output.status).into());
    }
    let metadata = String::from_utf8(output.stdout)?;
    let manifest = metadata
        .split("\"manifest_path\":\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .find(|path| path.ends_with("/reactor-webrtc-sys/Cargo.toml"))
        .ok_or("cargo metadata names no reactor-webrtc-sys manifest")?;
    // `<checkout>/crates/reactor-webrtc-sys/Cargo.toml`, as its build script reads it.
    let version = Path::new(manifest)
        .ancestors()
        .nth(3)
        .ok_or("reactor-webrtc-sys is not inside a reactor-webrtc checkout")?
        .join("WEBRTC_VERSION");
    println!("cargo::rerun-if-changed={}", version.display());
    let source = fs::read_to_string(&version)
        .map_err(|error| format!("read {}: {error}", version.display()))?;
    prebuilt_tag(&source).map(Some)
}

/// `webrtc-<milestone>-<commit8>-p<patch>` from `WEBRTC_VERSION`'s variables.
fn prebuilt_tag(source: &str) -> BuildResult<String> {
    let (mut branch, mut commit, mut patch) = (None, None, "0");
    for line in source.lines().map(str::trim) {
        if line.starts_with('#') {
            continue;
        }
        if let Some(value) = line.strip_prefix("WEBRTC_BRANCH=") {
            branch = Some(value);
        } else if let Some(value) = line.strip_prefix("WEBRTC_COMMIT=") {
            commit = Some(value);
        } else if let Some(value) = line.strip_prefix("REACTOR_PATCH_LEVEL=") {
            patch = value;
        }
    }
    let branch = branch.ok_or("WEBRTC_VERSION names no WEBRTC_BRANCH")?;
    let commit = commit
        .and_then(|commit| commit.get(..8))
        .ok_or("WEBRTC_VERSION pins no WEBRTC_COMMIT")?;
    let milestone = branch.strip_prefix("branch-heads/").unwrap_or(branch);
    Ok(format!("webrtc-{milestone}-{commit}-p{patch}"))
}

/// A variable Cargo sets for every build script.
fn cargo_env(key: &str) -> BuildResult<String> {
    env::var(key).map_err(|error| format!("Cargo sets {key}: {error}").into())
}

/// SHA-256 over each source input, as `path NUL contents NUL` in path order.
fn source_sha256(root: &Path) -> BuildResult<String> {
    let mut files = SOURCE_FILES.map(str::to_owned).to_vec();
    files.extend(files_under(root, &root.join("src"))?);
    files.sort();
    // The directory as well, so a new source file reruns this script.
    println!("cargo::rerun-if-changed=src");
    let mut input = Vec::new();
    for file in files {
        println!("cargo::rerun-if-changed={file}");
        let contents = fs::read(root.join(&file))
            .map_err(|error| format!("read native build input {file}: {error}"))?;
        input.extend_from_slice(file.as_bytes());
        input.push(0);
        input.extend_from_slice(&contents);
        input.push(0);
    }
    sha256_hex(&input)
}

/// Every file under `directory`, as a `/`-separated path relative to `root`.
fn files_under(root: &Path, directory: &Path) -> BuildResult<Vec<String>> {
    let unreadable = |error: io::Error| format!("read {}: {error}", directory.display());
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).map_err(unreadable)? {
        let path = entry.map_err(unreadable)?.path();
        if path.is_dir() {
            files.extend(files_under(root, &path)?);
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .ok_or_else(|| format!("source path is not UTF-8: {}", path.display()))?;
            files.push(relative.replace('\\', "/"));
        }
    }
    Ok(files)
}

/// SHA-256 as hex, from the platform's tool rather than a build dependency.
fn sha256_hex(input: &[u8]) -> BuildResult<String> {
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
        .map_err(|error| {
            format!("a SHA-256 tool is required to identify native build inputs: {error}")
        })?;
    // The piped stdin closes at the end of this statement, ending the input.
    child
        .stdin
        .take()
        .ok_or("the hash tool has no stdin")?
        .write_all(input)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!("native source hash failed: {}", output.status).into());
    }
    let text = String::from_utf8(output.stdout)?;
    let digest = text.split_whitespace().next().unwrap_or_default();
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("unexpected SHA-256 digest {digest:?}").into());
    }
    Ok(digest.to_owned())
}

/// The first line of `program --version`.
fn tool_version(program: &str) -> BuildResult<String> {
    let output = Command::new(program)
        .arg("--version")
        .output()
        .map_err(|error| format!("could not inspect native build tool {program}: {error}"))?;
    if !output.status.success() {
        return Err(format!("native build tool {program} failed: {}", output.status).into());
    }
    let text = String::from_utf8(output.stdout)?;
    Ok(text.lines().next().unwrap_or_default().to_owned())
}

/// `value` as a JSON string literal.
fn json_string(value: &str) -> String {
    JsonString(value).to_string()
}

/// Formats a string as a JSON string literal.
struct JsonString<'a>(&'a str);

impl fmt::Display for JsonString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for ch in self.0.chars() {
            match ch {
                '"' => f.write_str("\\\"")?,
                '\\' => f.write_str("\\\\")?,
                '\n' => f.write_str("\\n")?,
                '\r' => f.write_str("\\r")?,
                '\t' => f.write_str("\\t")?,
                ch if ch.is_control() => write!(f, "\\u{:04x}", u32::from(ch))?,
                ch => f.write_char(ch)?,
            }
        }
        f.write_char('"')
    }
}

/// The pinned Reactor libwebrtc archive contains `ScreenCaptureKit` objects
/// produced by Clang. They reference compiler-rt's deployment-version helper,
/// which rustc's macOS link line does not add on its own.
fn link_compiler_rt() -> BuildResult<()> {
    let cc = env::var("CC").unwrap_or_else(|_| "clang".to_owned());
    let output = Command::new(cc)
        .arg("-print-resource-dir")
        .output()
        .map_err(|error| {
            format!("clang is required to link the Reactor libwebrtc archive on macOS: {error}")
        })?;
    if !output.status.success() {
        return Err(format!("clang -print-resource-dir failed: {}", output.status).into());
    }
    let resource = String::from_utf8(output.stdout)?;
    let directory = PathBuf::from(resource.trim()).join("lib/darwin");
    let runtime = directory.join("libclang_rt.osx.a");
    if !runtime.is_file() {
        return Err(format!("missing macOS compiler-rt archive at {}", runtime.display()).into());
    }
    println!("cargo::rustc-link-search=native={}", directory.display());
    println!("cargo::rustc-link-lib=static=clang_rt.osx");
    // Cargo passes rustc-link-lib only to the library target, and the far-peer
    // example cannot link this cdylib, so it names the archive itself.
    println!("cargo::rustc-link-arg-examples={}", runtime.display());
    Ok(())
}
