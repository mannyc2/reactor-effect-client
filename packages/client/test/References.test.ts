/** References from data URIs, real files and a local server: bounded, timed, and never named. */
// A fixture server is Node's own http module, which Node and Bun both provide.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import { assert, layer } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Fiber,
  FileSystem,
  Inspectable,
  Layer,
  Path,
  Predicate,
  Tracer,
} from "effect";
import * as Base64 from "effect/encoding/Base64";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as TestClock from "effect/testing/TestClock";
import { ReactorTest, References } from "../src/index.js";

/** Answers a request, or keeps it waiting. */
type Route = (request: IncomingMessage, response: ServerResponse) => void;

/** A server on a free local port that answers every request by `route`, closed with the scope. */
const serve = Effect.fnUntraced(function* (route: Route) {
  const server = yield* Effect.acquireRelease(
    Effect.callback<Server>((resume) => {
      const created = createServer(route);
      created.listen(0, "127.0.0.1", () => resume(Effect.succeed(created)));
    }),
    // A request kept waiting holds its connection open, so every connection closes with it.
    (server) =>
      Effect.callback<void>((resume) => {
        server.closeAllConnections();
        server.close(() => resume(Effect.void));
      }),
  );
  const address = server.address();
  if (address === null || Predicate.isString(address))
    return yield* Effect.die("the fixture server has no port");
  return address.port;
});

const png = ReactorTest.pngBytes({ width: 64, height: 64 });
const dataUri = `data:image/png;base64,${Base64.encode(png)}`;

layer(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer, FetchHttpClient.layer))(
  "References",
  (it) => {
    it.effect("loads the same image from a data URI, a file and a URL", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        // A space in its name, which its file URI percent-encodes.
        const file = path.join(yield* fs.makeTempDirectoryScoped(), "a still.png");
        yield* fs.writeFile(file, png);
        const port = yield* serve((_, response) => response.end(png));
        const images = yield* Effect.forEach(
          [dataUri, (yield* path.toFileUrl(file)).href, `http://127.0.0.1:${port}/still.png`],
          (uri) => References.image(uri),
        );
        const still = { mimeType: "image/png", size: png.length, width: 64, height: 64 } as const;
        assert.deepStrictEqual(
          images.map(({ mimeType, size, width, height }) => ({ mimeType, size, width, height })),
          [still, still, still],
        );
      }),
    );

    it.effect("refuses a reference larger than its limit", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const file = path.join(yield* fs.makeTempDirectoryScoped(), "still.png");
        yield* fs.writeFile(file, png);
        const port = yield* serve((_, response) => response.end(png));
        const limit = { maxBytes: 1024 };
        const refusals = [
          yield* Effect.flip(References.image(dataUri, limit)),
          yield* Effect.flip(References.image(References.file(file), limit)),
          yield* Effect.flip(References.image(`http://127.0.0.1:${port}/still.png`, limit)),
        ];
        assert.deepStrictEqual(
          refusals.map((error) => [error.reason._tag, error.context.operation]),
          [
            ["InvalidInput", "reference"],
            ["InvalidInput", "reference"],
            ["InvalidInput", "reference"],
          ],
        );
      }),
    );

    it.effect("never puts the location in an error or a span", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        // A refusal that quotes what it refused, as some object stores' replies do.
        const port = yield* serve((request, response) => {
          response.statusCode = 403;
          response.end(`denied: ${request.url ?? ""}`);
        });
        const missing = path.join(yield* fs.makeTempDirectoryScoped(), "SECRET.png");
        const spans: Array<Tracer.NativeSpan> = [];
        const tracer = Tracer.make({
          span: (options) => {
            const span = new Tracer.NativeSpan(options);
            spans.push(span);
            return span;
          },
        });
        const errors = yield* Effect.forEach(
          [`http://127.0.0.1:${port}/x?signature=SECRET`, missing, "http://user:SECRET@host/"],
          (uri) => Effect.flip(References.image(uri)),
        ).pipe(Effect.withTracer(tracer));
        assert.deepStrictEqual(
          errors.map(({ reason }) =>
            reason._tag === "Http" ? [reason._tag, reason.status] : [reason._tag],
          ),
          [["Http", 403], ["InvalidInput"], ["InvalidInput"]],
        );
        assert.includeMembers(
          spans.map((span) => span.name),
          ["References.image", "References.locate", "References.read"],
        );
        const texts = [
          ...errors.flatMap((error) => [error.message, String(error), JSON.stringify(error)]),
          ...spans.map(
            (span) =>
              `${span.name} ${Inspectable.toStringUnknown(Object.fromEntries(span.attributes))}`,
          ),
        ];
        assert.deepStrictEqual(
          texts.filter((text) => text.includes("SECRET") || text.includes(String(port))),
          [],
        );
      }),
    );

    it.effect("gives up after its deadline", () =>
      Effect.gen(function* () {
        const arrived = yield* Deferred.make<void>();
        // Taken, and never answered.
        const port = yield* serve(() => {
          Deferred.doneUnsafe(arrived, Effect.void);
        });
        const location = yield* References.url(`http://127.0.0.1:${port}/slow`);
        const reading = yield* Effect.forkScoped(
          References.read(location, { timeout: "1 second" }),
        );
        yield* Deferred.await(arrived);
        yield* TestClock.adjust("1 second");
        const error = yield* Effect.flip(Fiber.join(reading));
        assert.strictEqual(error.reason._tag, "Timeout");
      }),
    );
  },
);
