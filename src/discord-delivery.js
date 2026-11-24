const { MessageFlags } = require("discord.js");

const DISCORD_CONTENT_LIMIT = 2000;
const MAX_PENDING_DISCORD_EVENTS = 256;
const MAX_PENDING_DISCORD_BYTES = 4 * 1024 * 1024;
// No destination may hold more than half the retained capacity, so a flood
// into one channel (e.g. a game-side feed) cannot starve the others.
const MAX_PENDING_EVENTS_PER_DESTINATION = MAX_PENDING_DISCORD_EVENTS / 2;
const MAX_PENDING_BYTES_PER_DESTINATION = MAX_PENDING_DISCORD_BYTES / 2;

/** Splits valid UTF-16 at code-point boundaries, including the author prefix. */
function* discordChunks(content) {
    for (let start = 0; start < content.length;) {
        let end = Math.min(start + DISCORD_CONTENT_LIMIT, content.length);
        const last = content.charCodeAt(end - 1);
        if (end < content.length && last >= 0xd800 && last <= 0xdbff) end--;
        const chunk = content.slice(start, end);
        // Discord cannot display empty/whitespace-only posts.
        if (chunk.trim()) yield chunk;
        start = end;
    }
}

/** Bounds retained events globally and serializes sends within each destination. */
class DiscordDelivery {
    constructor({ onSent, onDrop, logger }) {
        this.onSent = onSent;
        this.onDrop = onDrop;
        this.logger = logger;
        this.queues = new Map();
        this.pendingBytes = 0;
        this.pendingEvents = 0;
        this.closed = false;
    }

    /** Accepts a whole delivery event or drops it before sending any part. */
    enqueue(id, channel, content) {
        if (this.closed) return false;
        const text = content.toWellFormed();
        const event = { id, channel, content: text, bytes: Buffer.byteLength(text, "utf8") };
        const existing = this.queues.get(id);
        const destinationEvents = existing ? existing.length : 0;
        const destinationBytes = existing ? existing.pendingBytes : 0;
        if (!text.trim() || this.pendingEvents >= MAX_PENDING_DISCORD_EVENTS
            || event.bytes > MAX_PENDING_DISCORD_BYTES - this.pendingBytes
            || destinationEvents >= MAX_PENDING_EVENTS_PER_DESTINATION
            || event.bytes > MAX_PENDING_BYTES_PER_DESTINATION - destinationBytes) {
            this.drop(event, "empty content or queue capacity exceeded");
            return false;
        }
        let queue = existing;
        const start = !queue;
        if (!queue) {
            queue = [];
            queue.pendingBytes = 0;
            this.queues.set(id, queue);
        }
        queue.push(event);
        event.queue = queue;
        queue.pendingBytes += event.bytes;
        this.pendingBytes += event.bytes;
        this.pendingEvents++;
        if (start) this.drain(id, queue);
        return true;
    }

    /** Counts an incomplete delivery once without logging text or API request bodies. */
    drop(event, reason) {
        if (event.dropped) return;
        event.dropped = true;
        this.onDrop();
        this.logger.error(`Dropped MUD event for Discord channel ${event.id}: ${reason}`);
    }

    /** Releases retained accounting once, including when shutdown races a send. */
    release(event) {
        if (event.released) return;
        event.released = true;
        event.queue.pendingBytes -= event.bytes;
        this.pendingBytes -= event.bytes;
        this.pendingEvents--;
    }

    /** Sends one part at a time; failures discard the rest of that event without replay. */
    async drain(id, queue) {
        while (!this.closed && queue.length) {
            const event = queue[0];
            try {
                for (const content of discordChunks(event.content)) {
                    if (this.closed) break;
                    await event.channel.send({
                        content, allowedMentions: { parse: [] }, flags: MessageFlags.SuppressEmbeds
                    });
                    this.onSent();
                }
            } catch {
                this.drop(event, "send failed; remaining parts discarded");
            } finally {
                this.release(event);
                if (queue[0] === event) queue.shift();
            }
        }
        if (this.queues.get(id) === queue) this.queues.delete(id);
    }

    /** Drops pending events; an already-issued API request may still settle. */
    close() {
        this.closed = true;
        for (const queue of this.queues.values()) {
            for (const event of queue) {
                this.drop(event, "bridge stopped");
                this.release(event);
            }
            queue.length = 0;
        }
        this.queues.clear();
    }
}

module.exports = {
    DiscordDelivery, DISCORD_CONTENT_LIMIT, MAX_PENDING_DISCORD_EVENTS, MAX_PENDING_DISCORD_BYTES,
    MAX_PENDING_EVENTS_PER_DESTINATION, MAX_PENDING_BYTES_PER_DESTINATION
};
