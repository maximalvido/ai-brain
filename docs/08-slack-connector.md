# Slack Export Connector

Status: Supported export importer and live polling connector

This connector supports two ingestion modes:

- administrator-export import for broad initial or recovery ingestion;
- Slack Web API polling for recovery synchronization;
- Slack Events API webhooks for low-latency public-channel messages.

## Slack setup

An authorized Slack Workspace Owner or Enterprise Org Owner must create an
export that includes the public channels required for the import. Slack export
availability depends on the workspace plan and administrator approval. The
importer reads public-channel message files only; private channels, direct
messages, and group direct messages in the archive are ignored. See Slack's
[export workspace data
guide](https://slack.com/help/articles/201658943-export-your-workspace-data)
and [export format guide](https://slack.com/help/articles/220556107-How-to-read-Slack-data-exports).

Keep the downloaded ZIP outside version control and transfer it only to the
private AI Brain host. Do not place it in fixtures, `docs/`, or a shared issue.

## AI Brain setup

AI Brain needs a running PostgreSQL database and `DATABASE_URL`. No Slack API
token is required for this export-based importer.

Run the import from the repository root:

```bash
cargo run -p brain-server -- import-slack-export /secure/path/slack-export.zip
```

A previously extracted export directory is also accepted:

```bash
cargo run -p brain-server -- import-slack-export /secure/path/slack-export/
```

The command records Slack messages as `slack_message` observations and
resources. Re-running the same export is idempotent. Slack file contents are
not downloaded; the message payload retains source file links when they are
present in the export.

## Live API setup

Create a Slack app for the workspace and install it with the read scopes for
the conversations it should expose to AI Brain. For the public-channel-only
default, grant only:

- `channels:read` and `channels:history` for public channels;

Slack documents these scopes through the [Conversations API](https://api.slack.com/apis/conversations-api)
and [conversation history method](https://api.slack.com/methods/conversations.history).
Slack scopes are the authorization boundary. `SLACK_CONVERSATION_TYPES` only
controls which authorized conversation types the connector asks Slack to list;
it does not grant access. Keep the app limited to public-channel scopes when
private data must never be available to the connector.

Enable Event Subscriptions in the Slack app and set the Request URL to
`https://your-brain-host.example/webhooks/slack`. Subscribe the app to
`message.channels` so public-channel message events are delivered. AI Brain
verifies `X-Slack-Signature` and `X-Slack-Request-Timestamp` before ingesting
the raw event body. Slack URL verification is supported during setup.

```bash
export SLACK_API_TOKEN='xoxb-...'
export SLACK_CONVERSATION_TYPES='public_channel'
export SLACK_INITIAL_LOOKBACK_SECONDS=604800
export SLACK_SYNC_INTERVAL_SECONDS=300
```

Run one synchronization with:

```bash
cargo run -p brain-server -- sync-slack
```

Run the recurring worker with:

```bash
cargo run -p brain-server -- sync-slack-loop
```

Start this worker from Compose with the Slack profile after setting
`SLACK_API_TOKEN`:

```bash
docker compose --profile slack up -d slack-sync
```

## Current scope

- Public channels are the supported default for both export import and live
  API polling. Private channels, DMs, and group DMs are not imported from
  exports unless the implementation is explicitly extended for that policy.
- Webhook delivery covers public-channel message creation, edits, and
  deletions. Polling remains necessary for recovery when deliveries are
  delayed or missed.
- If one conversation history request fails, the poller logs the conversation
  and continues with the remaining channels. The run reports partial failure
  in its worker logs instead of abandoning the entire synchronization.
- Source permissions are not yet modeled per user.
- Secret detection and redaction are not yet implemented.
- The importer does not process Slack edits and deletions as delete events.

The live poller uses an initial seven-day lookback by default. Change
`SLACK_INITIAL_LOOKBACK_SECONDS` for a different first synchronization window.
Slack may apply strict history rate limits to non-Marketplace apps, so broad
workspaces may require a longer interval or an approved rate-limit increase.
