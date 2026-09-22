import * as Browser from "reactor-effect-client/browser";
import type { SessionOptions } from "reactor-effect-client";

const browserLayer = Browser.layer;
const connect = Browser.connect;
const options = {} as SessionOptions;

void browserLayer;
void connect;
void options;
