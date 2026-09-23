#!/usr/bin/env python3
"""Regenerate the portable wire codec from tracked canonical inputs.

Toolchain: CPython 3.13 (qualified with 3.13.5), standard library only; generator
format reactor-wire-ts/1. Google Struct uses the tracked protoc 3.13.0 descriptor
fixture, while every Reactor declaration is parsed from its tracked .proto file.
The parser deliberately rejects syntax outside that schema subset. There is no
download, mutable registry lookup, host protoc, or generated-TS template input.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import difflib
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
FORMAT_VERSION = "reactor-wire-ts/1"
WIRE_VERSION = "1.20260722.6"
SCALARS = {
    "string": ("string", '""', 2),
    "int64": ("bigint", "0n", 0),
    "int32": ("number", "0", 0),
    "double": ("number", "0", 1),
    "bool": ("boolean", "false", 0),
}


@dataclass
class Field:
    name: str
    type: str
    number: int
    repeated: bool = False
    oneof: str | None = None


@dataclass
class Message:
    name: str
    fields: list[Field] = field(default_factory=list)
    nested: list[Message] = field(default_factory=list)
    map_entry: bool = False


@dataclass
class Schema:
    name: str
    package: str
    imports: list[str] = field(default_factory=list)
    messages: list[Message] = field(default_factory=list)
    enums: dict[str, list[tuple[str, int]]] = field(default_factory=dict)


def descriptor_fields(data: bytes) -> list[tuple[int, int | bytes]]:
    """Bounded descriptor-only protobuf reader, independent of SDK wire code."""
    offset = 0

    def varint() -> int:
        nonlocal offset
        result = 0
        for shift in range(0, 70, 7):
            if offset >= len(data):
                raise ValueError("truncated descriptor varint")
            byte = data[offset]
            offset += 1
            result |= (byte & 127) << shift
            if byte < 128:
                return result
        raise ValueError("descriptor varint exceeds 64 bits")

    output = []
    while offset < len(data):
        tag = varint()
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError("invalid descriptor tag")
        if wire == 0:
            value = varint()
        elif wire in (1, 2, 5):
            length = varint() if wire == 2 else 8 if wire == 1 else 4
            if length > len(data) - offset:
                raise ValueError("truncated descriptor field")
            value = data[offset:offset + length]
            offset += length
        else:
            raise ValueError(f"unsupported descriptor wire type {wire}")
        output.append((number, value))
    return output


def desc_one(data: bytes, number: int, default=None):
    values = [value for key, value in descriptor_fields(data) if key == number]
    if len(values) > 1:
        raise ValueError(f"duplicate singular descriptor field {number}")
    return values[0] if values else default


def desc_text(data: bytes, number: int) -> str:
    value = desc_one(data, number)
    if not isinstance(value, bytes):
        raise ValueError(f"missing descriptor string {number}")
    return value.decode("utf-8")


def google_schema(path: Path) -> Schema:
    files = [value for number, value in descriptor_fields(path.read_bytes()) if number == 1]
    if len(files) != 1 or not isinstance(files[0], bytes):
        raise ValueError("expected one tracked Google Struct FileDescriptorProto")
    data = files[0]
    schema = Schema(desc_text(data, 1), desc_text(data, 2))
    if schema.name != "google/protobuf/struct.proto" or schema.package != "google.protobuf":
        raise ValueError("the tracked well-known descriptor is not Google Struct")

    def message(data: bytes) -> Message:
        result = Message(desc_text(data, 1))
        oneofs = [desc_text(value, 1) for number, value in descriptor_fields(data) if number == 8]
        for number, value in descriptor_fields(data):
            if number == 2:
                code = desc_one(value, 5)
                scalar = {1: "double", 3: "int64", 5: "int32", 8: "bool", 9: "string"}.get(code)
                type_name = scalar if scalar is not None else desc_text(value, 6)
                if code not in (1, 3, 5, 8, 9, 11, 14):
                    raise ValueError(f"unsupported Struct descriptor field type {code}")
                oneof_index = desc_one(value, 9)
                result.fields.append(Field(desc_text(value, 1), type_name, desc_one(value, 3),
                                           desc_one(value, 4) == 3,
                                           None if oneof_index is None else oneofs[oneof_index]))
            elif number == 3:
                result.nested.append(message(value))
            elif number == 7:
                result.map_entry = desc_one(value, 7, 0) == 1
        return result

    for number, value in descriptor_fields(data):
        if number == 4:
            schema.messages.append(message(value))
        elif number == 5:
            schema.enums[desc_text(value, 1)] = [
                (desc_text(item, 1), desc_one(item, 2))
                for key, item in descriptor_fields(value) if key == 2
            ]
    return schema


class ProtoParser:
    def __init__(self, path: Path, name: str):
        source = re.sub(r"//[^\n]*|/\*.*?\*/", "", path.read_text(), flags=re.S)
        pattern = re.compile(r'\s*("(?:\\.|[^"\\])*"|[A-Za-z_][\w.]*|-?\d+|[{};<>=,])')
        self.tokens = []
        position = 0
        while position < len(source):
            match = pattern.match(source, position)
            if match is None:
                if source[position:].strip():
                    raise ValueError(f"unsupported proto syntax in {name}: {source[position:position + 60]!r}")
                break
            self.tokens.append(match.group(1))
            position = match.end()
        self.offset = 0
        self.schema = Schema(name, "")

    def take(self, expected: str | None = None) -> str:
        if self.offset >= len(self.tokens):
            raise ValueError(f"unexpected end of {self.schema.name}")
        token = self.tokens[self.offset]
        self.offset += 1
        if expected is not None and expected != token:
            raise ValueError(f"{self.schema.name}: expected {expected!r}, found {token!r}")
        return token

    def peek(self) -> str:
        return self.tokens[self.offset] if self.offset < len(self.tokens) else ""

    def field(self, owner: Message, oneof: str | None = None) -> None:
        token = self.take()
        repeated = token == "repeated"
        if repeated:
            token = self.take()
        if token == "map":
            if oneof is not None or repeated:
                raise ValueError("map cannot be repeated or belong to a oneof")
            self.take("<")
            key = self.take()
            self.take(",")
            value = self.take()
            self.take(">")
            name = self.take()
            nested_name = "".join(part[:1].upper() + part[1:] for part in name.split("_")) + "Entry"
            nested = Message(nested_name, [Field("key", key, 1), Field("value", value, 2)], map_entry=True)
            owner.nested.append(nested)
            token, repeated = f"{owner.name}.{nested_name}", True
        else:
            name = self.take()
        self.take("=")
        number = int(self.take())
        self.take(";")
        if number <= 0 or any(item.number == number or item.name == name for item in owner.fields):
            raise ValueError(f"invalid/duplicate field {name} in {owner.name}")
        owner.fields.append(Field(name, token, number, repeated, oneof))

    def parse(self) -> Schema:
        while self.peek():
            token = self.take()
            if token == "syntax":
                self.take("="); self.take('"proto3"'); self.take(";")
            elif token == "package":
                self.schema.package = self.take(); self.take(";")
            elif token == "import":
                self.schema.imports.append(json.loads(self.take())); self.take(";")
            elif token == "enum":
                name = self.take(); self.take("{")
                values = []
                while self.peek() != "}":
                    key = self.take(); self.take("="); value = int(self.take()); self.take(";")
                    values.append((key, value))
                self.take("}")
                self.schema.enums[name] = values
            elif token == "message":
                owner = Message(self.take()); self.take("{")
                while self.peek() != "}":
                    if self.peek() == "oneof":
                        self.take(); name = self.take(); self.take("{")
                        while self.peek() != "}":
                            self.field(owner, name)
                        self.take("}")
                    else:
                        self.field(owner)
                self.take("}")
                self.schema.messages.append(owner)
            else:
                raise ValueError(f"unsupported proto declaration {token}")
        if self.schema.package != "reactor_wire.v1":
            raise ValueError("canonical Reactor package changed")
        return self.schema


def generate() -> str:
    files = {"google/protobuf/struct.proto": google_schema(ROOT / "wire/struct-descriptor.pb")}
    for path in sorted((ROOT / "wire/proto").rglob("*.proto")):
        name = path.relative_to(ROOT / "wire/proto").as_posix()
        files[name] = ProtoParser(path, name).parse()
    ordered: list[Schema] = []
    visited: set[str] = set()
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"cyclic proto import: {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in files[name].imports:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        ordered.append(files[name])

    for name in sorted(files):
        if name.startswith("reactor_wire/"):
            visit(name)

    messages: dict[str, tuple[str, Message]] = {}
    enums: dict[str, tuple[str, list[tuple[str, int]]]] = {}
    for schema in ordered:
        prefix = "Google_" if schema.package == "google.protobuf" else ""
        for name, values in schema.enums.items():
            enums[f"{schema.package}.{name}"] = (prefix + name, values)

        def collect(message: Message, parents: str = "") -> None:
            full_name = parents + message.name
            messages[f"{schema.package}.{full_name}"] = (prefix + full_name.replace(".", "_"), message)
            for nested in message.nested:
                collect(nested, full_name + ".")

        for message in schema.messages:
            collect(message)

    def resolve_type(owner: str, raw: str) -> str:
        if raw in SCALARS:
            return raw
        if raw.startswith("."):
            value = raw[1:]
        elif raw.startswith("google.protobuf."):
            value = raw
        else:
            package = "google.protobuf" if owner.startswith("google.protobuf.") else "reactor_wire.v1"
            value = package + "." + raw
        if value not in messages and value not in enums:
            raise ValueError(f"unresolved proto type {raw} in {owner}")
        return value

    def ts_type(value: str) -> str:
        return SCALARS[value][0] if value in SCALARS else "number" if value in enums else messages[value][0]

    def scalar_type(value: str) -> str:
        return "int32" if value in enums else value

    def default(value: str) -> str:
        return SCALARS[scalar_type(value)][1]

    out = [
        f"// GENERATED by wire/generate.py from canonical wire {WIRE_VERSION}. DO NOT EDIT.",
        "// Reactor proto definitions: Apache-2.0; google.protobuf definitions: BSD-3-Clause. See NOTICE.",
        'import { Reader, Writer } from "./protobuf.js";',
        'import type { WireLimits, UnknownFields } from "./protobuf.js";',
    ]
    for name, values in enums.values():
        out.append(f"export const {name} = {{ " + ", ".join(f"{key}: {value}" for key, value in values) + " } as const;")
    for owner, (name, message) in messages.items():
        fields = [(item, resolve_type(owner, item.type)) for item in message.fields]
        groups: dict[str, list[tuple[Field, str]]] = {}
        for item, value in fields:
            if item.oneof:
                groups.setdefault(item.oneof, []).append((item, value))

        def map_value(value: str) -> str | None:
            entry = messages.get(value)
            return resolve_type(value, entry[1].fields[1].type) if entry and entry[1].map_entry else None

        def write_value(number: int, value: str, expression: str) -> str:
            if value in messages:
                return f"{{ const messageValue = {expression}; w.message({number}, (w) => write{ts_type(value)}(w, messageValue)); }}"
            return f"w.{scalar_type(value)}({number}, {expression});"

        def read_value(value: str, base: str) -> str:
            return f"read{ts_type(value)}(r.child(), {base})" if value in messages else f"r.{scalar_type(value)}()"

        out.append(f"export interface {name} extends UnknownFields {{")
        for item, value in fields:
            if item.oneof:
                continue
            mapped = map_value(value)
            field_type = f"Map<string, {ts_type(mapped)}>" if mapped else f"Array<{ts_type(value)}>" if item.repeated else ts_type(value)
            optional = "?" if value in messages and not item.repeated else ""
            out.append(f"  {item.name}{optional}: {field_type};")
        for group, entries in groups.items():
            variants = " | ".join(f'{{ case: "{item.name}"; value: {ts_type(value)} }}' for item, value in entries)
            out.append(f"  {group}?: {variants};")
        out.extend(["}", f"export const {name} = {{",
            f"  encode(value: {name}, limits?: WireLimits): Uint8Array<ArrayBuffer> {{ const w = new Writer(limits); write{name}(w, value); return w.finish(); }},",
            f"  decode(bytes: Uint8Array, limits?: WireLimits): {name} {{ return read{name}(new Reader(bytes, limits)); }},",
            "};", f"function write{name}(w: Writer, value: {name}): void {{"])
        for item, value in fields:
            mapped = map_value(value)
            expression = f"value.{item.name}"
            if mapped:
                out.append(f"  for (const [key, item] of {expression}) w.message({item.number}, (w) => write{ts_type(value)}(w, {{ key, value: item }}));")
                continue
            if item.oneof:
                condition = f'if (value.{item.oneof}?.case === "{item.name}")'
                expression = f"value.{item.oneof}.value"
            elif item.repeated:
                condition = f"for (const item of {expression})"
                expression = "item"
            elif value in messages:
                condition = f"if ({expression} !== undefined)"
            else:
                extra = f" || Object.is({expression}, -0)" if value == "double" else ""
                condition = f"if ({expression} !== {default(value)}{extra})"
            out.extend([f"  {condition} {{", "    " + write_value(item.number, value, expression), "  }"])
        out.extend(["  w.unknown(value);", "}", f"function read{name}(r: Reader, base?: {name}): {name} {{"])
        for item, value in fields:
            if item.oneof:
                continue
            mapped = map_value(value)
            if mapped:
                declaration = f"const {item.name}: Map<string, {ts_type(mapped)}> = new Map(base?.{item.name} ?? []);"
            elif item.repeated:
                declaration = f"const {item.name}: Array<{ts_type(value)}> = [...(base?.{item.name} ?? [])];"
            elif value in messages:
                declaration = f"let {item.name}: {ts_type(value)} | undefined = base?.{item.name};"
            else:
                declaration = f"let {item.name}: {ts_type(value)} = base?.{item.name} ?? {default(value)};"
            out.append("  " + declaration)
        for group in groups:
            out.append(f'  let {group}: {name}["{group}"] = base?.{group};')
        out.extend(["  const _unknown: Uint8Array[] = [...(base?._unknown ?? [])];", "  while (!r.done) { const f = r.field(); switch (f.number) {"])
        for item, value in fields:
            wire = 2 if value in messages else SCALARS[scalar_type(value)][2]
            out.append(f"    case {item.number}: {{ r.expect(f, {wire});")
            mapped = map_value(value)
            if mapped:
                statement = f"const entry = {read_value(value, 'undefined')}; {item.name}.set(entry.key, entry.value ?? {ts_type(mapped)}.decode(new Uint8Array()));"
            elif item.oneof:
                base = f'{item.oneof}?.case === "{item.name}" ? {item.oneof}.value : undefined'
                statement = f'{item.oneof} = {{ case: "{item.name}", value: {read_value(value, base)} }};'
            elif item.repeated:
                statement = f"{item.name}.push({read_value(value, 'undefined')});"
            else:
                statement = f"{item.name} = {read_value(value, item.name)};"
            out.extend(["      " + statement, "      break; }"])
        out.extend(["    default: _unknown.push(r.unknown(f)); break;", "  } }"])
        returned = ["_unknown"]
        for item, value in fields:
            if item.oneof:
                continue
            returned.append(f"...({item.name} === undefined ? {{}} : {{ {item.name} }})" if value in messages and not item.repeated else item.name)
        returned.extend(f"...({group} === undefined ? {{}} : {{ {group} }})" for group in groups)
        out.extend(["  return { " + ", ".join(returned) + " };", "}", ""])
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="regenerate in memory and fail on any byte difference")
    parser.add_argument("--output", type=Path, default=ROOT / "src/wire.generated.ts")
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 13):
        raise SystemExit("wire generation requires CPython 3.13; CI is pinned to 3.13.5")
    generated = generate()
    if args.check:
        existing = args.output.read_text()
        if generated != existing:
            sys.stderr.writelines(difflib.unified_diff(existing.splitlines(True), generated.splitlines(True),
                                                      fromfile=str(args.output), tofile="regenerated"))
            raise SystemExit("wire regeneration differs; inspect canonical input changes and regenerate")
    else:
        args.output.write_text(generated)
    print(f"wire-generation-ok format={FORMAT_VERSION} python={sys.version.split()[0]} sha256={hashlib.sha256(generated.encode()).hexdigest()}")


if __name__ == "__main__":
    main()
