use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
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
}
