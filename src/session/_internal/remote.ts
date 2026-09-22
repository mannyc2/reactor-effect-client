import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { terminal } from "../../contract.js";
import type { Descriptor } from "../../contract.js";
import type { CoordinatorClient } from "../../coordinator/_internal/client.js";
import { ReactorError } from "../../errors.js";
import type { SessionOptions } from "../../SessionTypes.js";
import type { SessionLifecycle } from "./lifecycle.js";

export interface KnownRemote {
  readonly ownership: "owned" | "attached";
  readonly id: string;
  descriptor?: Descriptor;
  connectionId?: number;
}

export type Remote = { readonly ownership: "allocating" | "unknown" } | KnownRemote;

export const isKnownRemote = (remote: Remote | undefined): remote is KnownRemote =>
  remote?.ownership === "owned" || remote?.ownership === "attached";

/** One allocation's evidence survives peer generations and interrupted acquisition. */
export class RemoteSession {
  private value: Remote | undefined;

  get current(): Remote | undefined {
    return this.value;
  }

  get id(): string | undefined {
    return isKnownRemote(this.value) ? this.value.id : undefined;
  }

  get isKnown(): boolean {
    return isKnownRemote(this.value);
  }

  requireKnown(): KnownRemote {
    if (!isKnownRemote(this.value)) throw new ReactorError("InvalidState", "no known session id");
    return this.value;
  }

  allocationLost(): void {
    if (this.value?.ownership === "allocating") this.value = { ownership: "unknown" };
  }

  allocate(
    options: SessionOptions,
    http: CoordinatorClient,
    lifecycle: SessionLifecycle,
  ): Effect.Effect<string, ReactorError> {
    const self = this;
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (lifecycle.isClosing)
          return yield* Effect.fail(
            new ReactorError("Closed", "session is closed", { outcome: "not-submitted" }),
          );
        if (isKnownRemote(self.value)) return self.value.id;
        if (self.value !== undefined)
          return yield* Effect.fail(
            new ReactorError(
              "InvalidState",
              "session allocation is already pending or unresolved",
              { outcome: "unknown" },
            ),
          );
        const intent = options.intent;
        if (intent._tag === "Attach") {
          self.value = {
            ownership: "attached",
            id: intent.sessionId,
            ...(intent.connectionId === undefined ? {} : { connectionId: intent.connectionId }),
          };
          return self.value.id;
        }
        self.value = { ownership: "allocating" };
        return yield* restore(
          http
            .create(intent.model, intent.extraArgs)
            .pipe(Effect.raceFirst(Deferred.await(lifecycle.closing))),
        ).pipe(
          Effect.map((descriptor) => {
            self.value = { ownership: "owned", id: descriptor.session_id, descriptor };
            return descriptor.session_id;
          }),
          Effect.onExit((exit) =>
            Exit.isFailure(exit) ? Effect.sync(() => self.allocationLost()) : Effect.void,
          ),
        );
      }),
    );
  }

  isKnownTerminal(): boolean {
    return (
      isKnownRemote(this.value) &&
      this.value.descriptor !== undefined &&
      terminal(this.value.descriptor.state)
    );
  }
}
