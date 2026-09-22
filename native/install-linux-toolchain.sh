#!/bin/sh
set -eu

expected_fingerprint=6084F3CF814B57C1CF12EFD515CF4D18AF4F7421
key_url=https://apt.llvm.org/llvm-snapshot.gpg.key
keyring=/usr/share/keyrings/apt.llvm.org.gpg
source_list=/etc/apt/sources.list.d/apt.llvm.org.list

die() {
  echo "reactor-native-linux-toolchain: $*" >&2
  exit 2
}

[ "$(id -u)" -eq 0 ] || die "must run as root; this installer is opt-in for Docker/CI and never invokes sudo itself"
[ -r /etc/os-release ] || die "cannot identify Linux distribution: /etc/os-release is unavailable"

# shellcheck disable=SC1091
. /etc/os-release
case "${ID:-}:${VERSION_CODENAME:-}" in
  debian:bookworm) suite=bookworm ;;
  ubuntu:noble) suite=noble ;;
  *) die "unsupported distribution ${ID:-unknown}:${VERSION_CODENAME:-unknown}; supported: Debian 12 bookworm, Ubuntu 24.04 noble" ;;
esac

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl gnupg

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
curl --fail --silent --show-error --location "$key_url" --output "$tmp/llvm-snapshot.gpg.key"

actual_fingerprint=$(gpg --batch --with-colons --show-keys --fingerprint "$tmp/llvm-snapshot.gpg.key" \
  | awk -F: '$1 == "fpr" { print toupper($10); exit }')
[ "$actual_fingerprint" = "$expected_fingerprint" ] \
  || die "apt.llvm.org signing-key fingerprint mismatch: expected $expected_fingerprint, got ${actual_fingerprint:-missing}"

mkdir -p "$(dirname "$keyring")" "$(dirname "$source_list")"
gpg --batch --yes --dearmor --output "$keyring" "$tmp/llvm-snapshot.gpg.key"
chmod 0644 "$keyring"
printf 'deb [signed-by=%s] https://apt.llvm.org/%s/ llvm-toolchain-%s-21 main\n' "$keyring" "$suite" "$suite" > "$source_list"

apt-get update
apt-get install -y --no-install-recommends clang-21

clang-21 --version
clang++-21 --version
