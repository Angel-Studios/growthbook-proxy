import { Request, Response } from "express";
import logger, { truncatePayloadForLogging } from "../logger";
import { Context } from "../../types";
import { featuresCache } from "../cache";
import { registrar } from "../registrar";
import { SSEChannel } from "./ssePubsub";

function versionOf(payload: unknown): number | undefined {
  const dateUpdated =
    payload && typeof payload === "object"
      ? (payload as Record<string, unknown>).dateUpdated
      : undefined;
  if (typeof dateUpdated !== "string") return undefined;
  const epoch = Date.parse(dateUpdated);
  return Number.isNaN(epoch) ? undefined : epoch;
}

interface ScopedChannel {
  apiKey: string;
  channel: SSEChannel;
}

export type EventType = "features" | "features-updated";

export class SSEManager {
  private scopedChannels = new Map<string, ScopedChannel>();

  private appContext: Context;
  private readonly contentVersions: boolean;

  constructor(appContext: Context) {
    this.appContext = appContext;
    const settings = appContext.cacheSettings;
    this.contentVersions =
      !!appContext.enableCache &&
      !!settings &&
      settings.cacheEngine !== "memory" &&
      settings.publishPayloadToChannel === true;
  }

  public async subscribe(req: Request, res: Response) {
    if (this.appContext?.verboseDebugging) {
      logger.info("EventStreamManager.subscribe");
    }
    const apiKey = res.locals.apiKey;
    if (apiKey) {
      let scopedChannel;
      try {
        scopedChannel = this.getScopedChannel(apiKey);
      } catch (e) {
        logger.error({ err: e }, "Unable to get SSE channel");
      }
      if (scopedChannel) {
        try {
          await this.seedFromCache(apiKey, scopedChannel);
          scopedChannel.channel.subscribe(req, res);

          if (this.appContext?.verboseDebugging) {
            logger.info(
              this.getSubscriberCounts(),
              `EventSource subscriber counts`,
            );
          }
        } catch (e) {
          logger.error({ err: e }, "Unable to subscribe to SSE channel");
        }
      } else {
        logger.error({ apiKey }, "Unable to get SSE channel");
      }
    }
  }

  private async seedFromCache(
    apiKey: string,
    scopedChannel: ScopedChannel,
  ): Promise<void> {
    if (!this.contentVersions) return;
    if (scopedChannel.channel.hasContent()) return;
    if (!featuresCache) return;
    const entry = await featuresCache.get(apiKey);
    if (!entry) return;
    if (scopedChannel.channel.hasContent()) return;
    const remoteEvalEnabled =
      !!registrar.getConnection(apiKey)?.remoteEvalEnabled;
    scopedChannel.channel.publish(
      remoteEvalEnabled ? "" : entry.payload,
      remoteEvalEnabled ? "features-updated" : "features",
      versionOf(entry.payload),
    );
  }

  public getSubscriberCount(apiKey: string): number | null {
    const scopedChannel = this.getScopedChannel(apiKey);
    if (scopedChannel) {
      return scopedChannel.channel.getSubscriberCount();
    }
    return null;
  }

  public getSubscriberCounts(): Record<string, number> {
    const counts: Record<string, number> = {};
    this.scopedChannels.forEach((scopedChannel) => {
      counts[scopedChannel.apiKey] = scopedChannel.channel.getSubscriberCount();
    });
    return counts;
  }

  public publish({
    apiKey,
    event,
    payload,
    oldPayload,
  }: {
    apiKey: string;
    event: EventType;
    /* eslint-disable @typescript-eslint/no-explicit-any */
    payload: any;
    /* eslint-disable @typescript-eslint/no-explicit-any */
    oldPayload?: any;
    remoteEvalEnabled?: boolean;
  }) {
    if (this.appContext?.verboseDebugging) {
      logger.info(
        {
          apiKey,
          event,
          payload: truncatePayloadForLogging(payload),
          oldPayload: truncatePayloadForLogging(oldPayload),
        },
        "EventStreamManager.publish",
      );
    }
    const scopedChannel = this.getScopedChannel(apiKey);
    if (!scopedChannel) {
      if (this.appContext?.verboseDebugging) {
        logger.info("No scoped channel found");
      }
      return;
    }

    const hasChanges = JSON.stringify(payload) !== JSON.stringify(oldPayload);
    if (!hasChanges) {
      if (this.appContext?.verboseDebugging) {
        logger.info(
          { payload: truncatePayloadForLogging(payload), event },
          "skipping SSE publish, no changes",
        );
      }
      return;
    }

    const version = versionOf(payload);
    if (event === "features-updated") {
      // change payload to a "please refetch" beacon
      payload = "";
    }
    scopedChannel.channel.publish(payload, event, version);
  }

  public closeAll() {
    this.scopedChannels.forEach((scopedChannel) => {
      scopedChannel.channel.close();
    });
  }

  private getScopedChannel(apiKey: string): ScopedChannel | undefined {
    let scopedChannel = this.scopedChannels.get(apiKey);
    if (!scopedChannel) {
      this.scopedChannels.set(apiKey, {
        apiKey,
        channel: new SSEChannel(
          {
            maxStreamDuration: this.appContext.eventStreamMaxDurationMs,
            pingInterval: this.appContext.eventStreamPingIntervalMs,
            contentVersions: this.contentVersions,
          },
          this.appContext,
        ),
      });
      scopedChannel = this.scopedChannels.get(apiKey);
    }
    return scopedChannel;
  }
}

export type EventStreamManager = SSEManager | null;
export let eventStreamManager: SSEManager | null = null;

export const initializeEventStreamManager = (appContext: Context) => {
  if (appContext.enableEventStream) {
    eventStreamManager = new SSEManager(appContext);
  }
  Object.freeze(eventStreamManager);
};
