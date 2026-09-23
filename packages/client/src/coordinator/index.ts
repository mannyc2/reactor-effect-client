import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Http from "effect/unstable/http/HttpClient";
import { parsed, ReactorError } from "../errors.js";
import type { Json } from "../json.js";
import { CoordinatorClient } from "./_internal/client.js";
import type { HttpOptions, Termination } from "./_internal/client.js";
import type { Inspection, TokenGrant, TokenOptions } from "./_internal/schemas.js";

export { Inspection, modelRate } from "./_internal/schemas.js";
export type { TokenGrant, TokenOptions } from "./_internal/schemas.js";
export type { Termination, Poll } from "./_internal/client.js";
export type {
  Capabilities,
  Descriptor,
  Track,
  Mapping,
  IceCandidate,
  IceServer,
} from "../contract.js";

export interface Configuration extends Omit<HttpOptions, "apiUrl" | "credential"> {
  readonly apiUrl?: string;
  /** Evaluated for each authenticated request, including a supervisor's confirmation GET. */
  readonly credential?: Effect.Effect<Redacted.Redacted<string>, ReactorError>;
}

/** Coordinator operations share configuration and the supplied Effect HTTP client. */
export interface Client {
  readonly apiUrl: string;
  readonly pricing: Effect.Effect<Json, ReactorError>;
  readonly mintToken: (options: TokenOptions) => Effect.Effect<TokenGrant, ReactorError>;
  readonly inspect: (sessionId: string) => Effect.Effect<Inspection, ReactorError>;
  /** Uncertainty is retained in the report; supervisors choose their own failure policy. */
  readonly terminate: (sessionId: string) => Effect.Effect<Termination>;
}

/** No network request or remote allocation occurs while constructing a coordinator client. */
export const make = (
  configuration: Configuration = {},
): Effect.Effect<Client, ReactorError, Http.HttpClient> =>
  Effect.flatMap(Http.HttpClient, (http) =>
    parsed(
      () =>
        new CoordinatorClient(
          {
            ...configuration,
            apiUrl: configuration.apiUrl ?? "https://api.reactor.inc",
            credential:
              configuration.credential === undefined
                ? Effect.undefined
                : configuration.credential.pipe(
                    Effect.flatMap((credential) =>
                      parsed(() => {
                        if (!Redacted.isRedacted(credential))
                          throw ReactorError.fromCode(
                            "InvalidInput",
                            "Coordinator credential must be Redacted",
                            { outcome: "not-submitted" },
                          );
                        return Redacted.value(credential);
                      }),
                    ),
                  ),
          },
          http,
        ),
    ),
  );
