import * as Effect from "effect/Effect";
import * as Client from "reactor-effect-client/Client";
import * as H3 from "reactor-effect-client/h3";
import * as Native from "reactor-effect-client/native";
import * as Sessions from "reactor-effect-client/Sessions";
import { ReactorError } from "reactor-effect-client";

const clientLayer = Client.layer();
const nativeLayer = Native.layer();
const sequence = H3.makeAffinity<string>();
const pricing = Sessions.pricing;

const values: readonly unknown[] = [clientLayer, nativeLayer, sequence, pricing, ReactorError, Effect.void];
void values;
