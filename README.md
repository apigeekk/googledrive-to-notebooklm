# Auto-Syncing Knowledge Base: Google Drive to NotebookLM

Google NotebookLM Enterprise is a powerful tool for querying and synthesizing team knowledge — but keeping it in sync with hundreds of evolving documents in Google Drive requires more than a manual upload. This pipeline automates the entire process, so your team always has a single, up-to-date knowledge base to query, without anyone thinking about maintenance.

## What It Does

- **Delta sync**: detects new and modified files nightly, skips untouched ones
- **Smart naming**: uses the Gemini API to apply a consistent `[Category] - [Title]` naming convention based on subfolder structure
- **Shortcut resolution**: handles Google Drive shortcuts transparently
- **Name caching**: avoids redundant Gemini API calls by caching generated names
- **Type filtering**: automatically skips file types unsupported by the NotebookLM Enterprise API
- **Decoupled workers**: split into two independent timed triggers to stay within Apps Script's 25-minute execution limit
- **Daily email reports**: each worker sends a nightly summary of what was copied, renamed, uploaded, or skipped

## How It Works

The pipeline runs on two nightly triggers:

| Worker | Function | Schedule |
|---|---|---|
| Worker A | Scans Drive, renames files, copies to staging | ~1:00 AM |
| Worker B | Compares staging to NotebookLM, uploads/deletes | ~2:00 AM |

For full setup instructions and technical implementation details, see the accompanying blog post:
**[Building an Auto-Syncing Knowledge Base with Google NotebookLM](https://medium.com/@apigeek/building-an-auto-syncing-knowledge-base-with-google-notebooklm-b4f465e90420)**

## Prerequisites

- NotebookLM Enterprise license
- Google Cloud Project with Discovery Engine API enabled
- Gemini API key (from Google AI Studio)
- Two Google Drive folders: a source folder and a staging folder

## Setup

1. Open [Google Apps Script](https://script.google.com/) and create a new project
2. Paste the contents of `Code.gs` into the editor
3. Fill in the `CONFIGURATION` block at the top of the script with your values
4. Configure the `appsscript.json` manifest with the required OAuth scopes (see blog post)
5. Run `runReplicatorOnly` and `runSyncOnly` once each to authorize permissions
6. Set up two daily time-based triggers for each worker function

## Configuration

| Variable | Description |
|---|---|
| `GCP_PROJECT_NUMBER` | Your GCP project number |
| `NOTEBOOK_ID` | UUID of your NotebookLM notebook (from the URL) |
| `SOURCE_MASTER_ID` | Drive ID of your source folder |
| `STAGING_FOLDER_ID` | Drive ID of your staging folder |
| `GEMINI_API_KEY` | Gemini API key from Google AI Studio |
| `RECIPIENT_EMAIL` | Email address for nightly reports |

## License

MIT
