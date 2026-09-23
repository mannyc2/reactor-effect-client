use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn source_files(root: &Path, directory: &Path, files: &mut Vec<String>) {
    for entry in fs::read_dir(directory).expect("native source directory must exist") {
        let path = entry.expect("read native source entry").path();
        if path.is_dir() {
            source_files(root, &path, files);
        } else if path.is_file() {
            files.push(
                path.strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/"),
            );
        }
    }
}

fn quote(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn version(program: &str) -> String {
    let output = Command::new(program)
        .arg("--version")
        .output()
        .unwrap_or_else(|_| panic!("could not inspect native build tool {program}"));
    assert!(
        output.status.success(),
        "native build tool {program} failed"
    );
    String::from_utf8(output.stdout)
        .expect("build tool version must be UTF-8")
        .lines()
        .next()
        .unwrap_or("")
        .to_owned()
}

fn build_identity() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let mut files = [
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        ".cargo/config.toml",
        "include/reactor_effect_native.h",
    ]
    .map(str::to_owned)
    .to_vec();
    source_files(&root, &root.join("src"), &mut files);
    files.sort();
    let mut input = Vec::new();
    for file in files {
        println!("cargo:rerun-if-changed={file}");
        input.extend_from_slice(file.as_bytes());
        input.push(0);
        input.extend_from_slice(&fs::read(root.join(file)).expect("read native build input"));
        input.push(0);
    }
    let mut command = if cfg!(target_os = "macos") {
        let mut command = Command::new("shasum");
        command.args(["-a", "256"]);
        command
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
        .unwrap()
        .write_all(&input)
        .expect("hash native build inputs");
    let output = child.wait_with_output().expect("join native source hash");
    assert!(output.status.success(), "native source hash failed");
    let text = String::from_utf8(output.stdout).unwrap();
    let digest = text.split_whitespace().next().unwrap();
    assert!(digest.len() == 64 && digest.chars().all(|ch| ch.is_ascii_hexdigit()));

    let cc = env::var("CC").unwrap_or_else(|_| "clang".into());
    let cxx = env::var("CXX").unwrap_or_else(|_| "clang++".into());
    let mut fields = vec![
        "\"schemaVersion\":1".to_owned(),
        "\"abiVersion\":3".to_owned(),
        format!("\"sourceSha256\":{}", quote(digest)),
        format!("\"target\":{}", quote(&env::var("TARGET").unwrap())),
        format!("\"profile\":{}", quote(&env::var("PROFILE").unwrap())),
        format!("\"rustc\":{}", quote(&version(&env::var("RUSTC").unwrap()))),
        format!("\"cc\":{}", quote(&version(&cc))),
        format!("\"cxx\":{}", quote(&version(&cxx))),
    ];
    for key in [
        "CC",
        "CXX",
        "CARGO_ENCODED_RUSTFLAGS",
        "CFLAGS",
        "CXXFLAGS",
        "MACOSX_DEPLOYMENT_TARGET",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
        if let Ok(value) = env::var(key) {
            fields.push(format!("{}:{}", quote(key), quote(&value)));
        }
    }
    println!(
        "cargo:rustc-env=REACTOR_EFFECT_BUILD_IDENTITY={{{}}}",
        fields.join(",")
    );
}

fn main() {
    build_identity();
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    // The pinned Reactor libwebrtc archive contains ScreenCaptureKit objects
    // produced by Clang. They reference compiler-rt's deployment-version helper,
    // which rustc's macOS link line does not add on its own.
    let cc = env::var("CC").unwrap_or_else(|_| "clang".to_owned());
    let output = Command::new(cc)
        .arg("-print-resource-dir")
        .output()
        .expect("clang is required to link the Reactor libwebrtc archive on macOS");
    assert!(output.status.success(), "clang -print-resource-dir failed");
    let resource = String::from_utf8(output.stdout).expect("clang resource path is not UTF-8");
    let directory = PathBuf::from(resource.trim()).join("lib/darwin");
    let runtime = directory.join("libclang_rt.osx.a");
    assert!(
        runtime.is_file(),
        "missing macOS compiler-rt archive at {}",
        runtime.display()
    );
    println!("cargo:rustc-link-search=native={}", directory.display());
    println!("cargo:rustc-link-lib=static=clang_rt.osx");
    // Cargo passes rustc-link-lib only to the library target, and the far-peer
    // example cannot link this cdylib, so it names the archive itself.
    println!("cargo:rustc-link-arg-examples={}", runtime.display());
}
