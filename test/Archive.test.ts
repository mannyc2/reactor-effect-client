import { test } from "bun:test";
import { tests } from "./harness.js";
import "./wire.test.js";
import "./http.test.js";
import "./session.test.js";
import "./session-client.test.js";
import "./media.test.js";
import "./publication.test.js";
import "./stats.test.js";
import "./recording.test.js";
import "./native-audio.test.js";

for (const entry of tests) test(entry.name, entry.body, 15_000);
