/** Playlist parsing rejects malformed provider data as a typed failure; a bug would stay a defect. */
import { parsePlaylist } from "../src/_internal/recording.js";
import { equal, test, throws } from "./harness.js";

test("recording policy: a malformed playlist URI is a typed Protocol failure", () => {
  throws(
    () => parsePlaylist("#EXTM3U\nhttp://[bad/segment.ts\n", "https://clips.fixture/"),
    "Protocol",
  );
  throws(() => parsePlaylist("#EXTM3U\n", "https://clips.fixture/"), "Protocol");
  equal(parsePlaylist("#EXTM3U\nsegment.ts\n", "https://clips.fixture/clip/"), [
    { kind: "media", url: "https://clips.fixture/clip/segment.ts" },
  ]);
});
