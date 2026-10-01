/**
 * The static playground: the Studio on the simulated Reactor alone. It needs
 * no server and no key, so any static host serves it from any path.
 */
import { ManagedRuntime } from "effect";
import * as Offline from "./Offline.ts";
import * as Page from "./Page.ts";
import * as Studio from "./Studio.ts";

Studio.mount(ManagedRuntime.make(Offline.layer({ screen: Page.ui.screen })));
