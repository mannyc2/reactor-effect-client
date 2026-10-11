import { Context } from "effect";
import type { H3, Playout, ReactorError, Session } from "reactor-effect-client";

/** The rundown's finite H3 playout, selected by the application entry point. */
export class Channel extends Context.Service<
  Channel,
  Playout.Service<H3.Request, Session.CloseReport, ReactorError.AcquisitionFailure>
>()("reactor-effect-client-examples/Channel") {}
