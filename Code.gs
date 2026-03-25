// ==========================================
//    ENTERPRISE KNOWLEDGE BASE SYNC WITH GOOGLE NOTEBOOKLM
//    
//
//  OVERVIEW:
//  This script maintains an auto-syncing knowledge base between a Google Drive
//  folder and a Google NotebookLM Enterprise notebook. It is split into two
//  independent workers that run on a predefined schedule:
//
//    Worker A (runReplicatorOnly) — Scans the source Drive folder, applies
//    AI-generated names via the Gemini API, and copies/renames files into a
//    flat staging folder. Runs first (e.g. 1:00 AM).
//
//    Worker B (runSyncOnly) — Compares the staging folder against the live
//    NotebookLM notebook, uploads new files, and deletes stale ones.
//    Runs after Worker A finishes (e.g. 2:00 AM).
//
//  This decoupled design avoids the 25-minute Google Apps Script execution
//  limit by splitting the heavy lifting across two separate timed triggers.
// ==========================================


// --- CONFIGURATION ---
// Replace each placeholder with your actual values before running.
const GCP_PROJECT_NUMBER = "GCP_PROJECT_NUMBER";   // GCP project where Discovery Engine API is enabled & NotebookLM notebook created
const NOTEBOOK_ID        = "NOTEBOOK_ID";           // UUID of your NotebookLM notebook (from its URL)
const SOURCE_MASTER_ID   = "SOURCE_MASTER_ID";      // Google Drive ID of your source folder (from its URL)
const STAGING_FOLDER_ID  = "STAGING_FOLDER_ID";     // Google Drive ID of your hidden staging folder (from its URL)
const GEMINI_API_KEY     = "GEMINI_API_KEY";        // Gemini API key from Google AI Studio (used for smart naming)
const RECIPIENT_EMAIL    = "RECIPIENT_EMAIL";       // Email address to receive daily execution reports

// AI Configuration
// GEMINI_MODEL: the model used for smart file naming. Update to a current
// public model (e.g. "gemini-2.0-flash") before deploying.
const GEMINI_MODEL   = "gemini-3-flash-preview";         // Replace this if nneeded
const GEMINI_VERSION = "v1beta";

// System Limits
const LOCATION          = "global";           // NotebookLM Enterprise API location
const TOTAL_LENGTH_LIMIT = 65;                // Max character length for AI-generated source file names in NotebookLM
const MAX_RUNTIME_MS     = 25 * 60 * 1000;   // 25-minute hard cap — matches Google Apps Script execution limit
const UPLOAD_BATCH_SIZE  = 2;                 // Number of files uploaded per API call. Keep low to avoid timeouts.

// Supported MIME types for NotebookLM ingestion.
// Files with types not in this list are automatically skipped by the pipeline.
// Note: you may need to update this list as NotebookLM keeps evolving.
const ALLOWED_MIME_TYPES = [
  "application/vnd.google-apps.document",       // Google Docs
  "application/vnd.google-apps.presentation",   // Google Slides
  "application/pdf",
  "text/plain",
  "text/markdown",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",    // .docx
  "application/vnd.openxmlformats-officedocument.presentationml.presentation",  // .pptx
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",          // .xlsx
  "audio/mpeg", "audio/wav", "audio/mp4", "video/mp4", "audio/x-m4a"
];


// ==========================================
// WORKER A: THE REPLICATOR (e.g. runs ~1:00 AM)
// ==========================================
// Responsibilities:
//  - Scans the source Drive folder (including subfolders and shortcuts)
//  - Generates smart names using the Gemini API (with local caching)
//  - Copies new/modified files into the staging folder with the smart name
//  - Renames staging files whose names have drifted from the convention
//  - Sends a daily email report on completion
// ==========================================

function runReplicatorOnly() {
  console.log("=== 🏗️ WORKER A STARTED: REPLICATOR ===");

  // Prevent concurrent runs — if another instance is already running, exit immediately.
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) { console.warn("🔒 LOCKED: Script is already running."); return; }

  const startTime = new Date().getTime();

  // Stats and error logs are scoped to this run only, so each nightly email
  // reflects only what happened tonight — not a cumulative total.
  let localStats    = { processed: 0, copied: 0, renamed: 0, skipped: 0 };
  let localFailures = []; // Per-file errors (copy failures, rename failures, permission issues)
  let localErrors   = []; // System-level errors (crashes, timeouts)

  try {
    const sourceFolder  = DriveApp.getFolderById(SOURCE_MASTER_ID);
    const stagingFolder = DriveApp.getFolderById(STAGING_FOLDER_ID);

    // --- STEP 1: Load the name cache ---
    // AI-generated names are persisted in PropertiesService to avoid calling
    // the Gemini API repeatedly for files that haven't changed. The cache key
    // is the source file's Drive ID; the value is the previously generated name.
    console.log("🔍 [INIT] Loading Name Cache...");
    const scriptProperties = PropertiesService.getScriptProperties();
    const nameCache        = scriptProperties.getProperties();

    // --- STEP 2: Map staging files by their source ID ---
    // Each staged file stores its source Drive ID in its description field
    // (format: "source:<driveId>"). This lets Worker A quickly look up whether
    // a source file already has a staged copy, and compare modification times.
    const stagingFiles = {};
    const sIter = stagingFolder.getFiles();
    while (sIter.hasNext()) {
      const f    = sIter.next();
      const desc = f.getDescription();
      if (desc && desc.startsWith("source:")) {
        stagingFiles[desc.split(":")[1]] = f;
      }
    }

    // --- STEP 3: Scan the source folder ---
    // Recursively walks the source folder one level deep. Uses subfolder names
    // as the "Category" prefix for smart naming. Resolves Drive shortcuts to
    // their target files. Skips unsupported MIME types.
    const sourceFiles = {};
    scanFolderRecursive(sourceFolder, null, sourceFiles);
    const sourceIds  = Object.keys(sourceFiles);
    const totalFiles = sourceIds.length;
    console.log(`✅ [INIT] Found ${totalFiles} source files.`);

    // --- STEP 4: Process each source file ---
    for (let i = 0; i < totalFiles; i++) {
      const id          = sourceIds[i];
      const progressTag = `[${i+1}/${totalFiles}]`;

      // Safety valve: stop gracefully if approaching the 25-minute Apps Script limit.
      // Files not reached tonight will be picked up on the next nightly run.
      if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) {
        console.warn(`⚠️ [TIMEOUT] Stopping Worker A.`);
        localErrors.push({ action: "TIMEOUT", file: "System", error: "25min limit reached" });
        break;
      }

      localStats.processed++;
      const data        = sourceFiles[id];
      let needsCopy     = false;
      let needsRename   = false;

      // Determine what action (if any) is needed for this file.
      if (stagingFiles[id]) {
        const stagedFile  = stagingFiles[id];
        const sourceTime  = new Date(data.modified).getTime();
        const stagedTime  = new Date(stagedFile.getLastUpdated()).getTime();

        if (sourceTime > stagedTime + 60000) {
          // Source file was modified more than 1 minute after the staged copy —
          // trash the old staged copy and re-copy from source.
          needsCopy = true;
          try { stagedFile.setTrashed(true); } catch(e) {}
        } else if (!stagedFile.getName().startsWith(`${data.category} - `)) {
          // The staged file exists and is current, but its name doesn't match
          // the expected "[Category] - [Title]" convention — rename it in place.
          needsRename = true;
        } else {
          // File is up-to-date and correctly named — nothing to do.
          localStats.skipped++;
        }
      } else {
        // No staged copy exists yet — this is a new file.
        needsCopy = true;
      }

      if (!needsCopy && !needsRename) continue;

      // --- Smart naming ---
      // Check the cache first to avoid unnecessary Gemini API calls.
      // If a cached name exists but its category prefix has changed (e.g. the
      // file was moved to a different subfolder), update the prefix accordingly.
      let smartName;
      if (nameCache[id]) {
        smartName = nameCache[id];
        if (!smartName.startsWith(data.category)) {
          let pureTitle = smartName.includes(" - ")
            ? smartName.split(" - ").slice(1).join(" - ")
            : smartName;
          smartName = `${data.category} - ${pureTitle}`;
        }
      } else {
        // No cached name — call Gemini to generate one, or fall back to basic
        // name cleaning if the file has no category (i.e. it's in the root folder).
        if (data.category) {
          smartName = callGeminiNaming(data.originalName, data.category, id);
          nameCache[id] = smartName;
        } else {
          smartName = cleanNameLogic(data.originalName, null);
        }
      }

      // --- Execute the required action ---
      if (needsCopy) {
        console.log(`${progressTag} 📂 [COPY] -> ${smartName}`);
        try {
          // Copy the original file into the staging folder with the smart name.
          // The description "source:<id>" is how Worker B and future Worker A
          // runs identify which source file this staged copy belongs to.
          const original = DriveApp.getFileById(data.targetId);
          original.makeCopy(smartName, stagingFolder).setDescription(`source:${id}`);
          localStats.copied++;
        } catch (e) {
          console.error(`${progressTag} ❌ [COPY FAIL] ${e}`);
          localFailures.push({ name: data.originalName, url: `https://drive.google.com/open?id=${data.targetId}`, reason: `COPY FAIL: ${e}` });
        }
      } else if (needsRename) {
        console.log(`${progressTag} ✏️ [RENAME] -> ${smartName}`);
        try {
          stagingFiles[id].setName(smartName);
          localStats.renamed++;
        } catch (e) {
          console.error(`${progressTag} ❌ [RENAME FAIL] ${e}`);
          localFailures.push({ name: data.originalName, url: `https://drive.google.com/open?id=${stagingFiles[id].getId()}`, reason: `RENAME FAIL: ${e}` });
        }
      }
    }

  } catch (e) {
    console.error("☠️ [CRITICAL WORKER A CRASH]", e);
    localErrors.push({ action: "CRASH", file: "Worker A", error: e.toString() });
  } finally {
    lock.releaseLock();
    sendDecoupledEmail("Worker A (Replicator)", localStats, localFailures, localErrors);
    console.log("=== ✅ WORKER A FINISHED ===");
  }
}


// ==========================================
// WORKER B: THE SYNC MOVER (runs ~2:00 AM)
// ==========================================
// Responsibilities:
//  - Reads the current state of the staging folder (the source of truth)
//  - Reads the current state of the live NotebookLM notebook via the API
//  - Deletes sources from NotebookLM that no longer exist in staging
//  - Uploads new staging files to NotebookLM (skipping unsupported types)
//  - Sends a daily email report on completion
//
//  Schedule Worker B at least 1 hour after Worker A to ensure staging
//  is fully populated before the sync begins.
// ==========================================

function runSyncOnly() {
  console.log("=== 🚚 WORKER B STARTED: SYNC ===");

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) { console.warn("🔒 LOCKED: Script is already running."); return; }

  const startTime    = new Date().getTime();
  // Build the base API endpoint for this notebook. All source operations
  // (list, batchCreate, batchDelete) are performed against this URL.
  const API_ENDPOINT = `https://${LOCATION}-discoveryengine.googleapis.com/v1alpha/projects/${GCP_PROJECT_NUMBER}/locations/${LOCATION}/notebooks/${NOTEBOOK_ID}`;

  let localStats    = { uploaded: 0, deleted: 0, skipped: 0 };
  let localFailures = [];
  let localErrors   = [];

  try {
    // --- STEP 1: Build a map of files currently in staging ---
    // Keyed by Drive file ID. This is the desired state — what NotebookLM should contain.
    console.log("🔍 [SYNC] Scanning Staging...");
    const stagingMap = {};
    const folder     = DriveApp.getFolderById(STAGING_FOLDER_ID);
    const files      = folder.getFiles();
    while (files.hasNext()) {
      const f = files.next();
      stagingMap[f.getId()] = { id: f.getId(), name: f.getName(), mimeType: f.getMimeType() };
    }

    // --- STEP 2: Build a map of sources currently in NotebookLM ---
    // Keyed by Drive file ID. This is the current state — what NotebookLM actually contains.
    console.log("🔍 [SYNC] Fetching NotebookLM...");
    const currentSources = getNotebookSources(API_ENDPOINT);
    const nbMap          = {};
    if (currentSources && currentSources.length > 0) {
      currentSources.forEach(s => {
        let did = null;
        // The API returns document IDs under different metadata paths depending
        // on how the source was originally uploaded — check both locations.
        if (s.metadata && s.metadata.googleDocsMetadata) {
          did = s.metadata.googleDocsMetadata.documentId;
        } else if (s.googleDriveContent) {
          did = s.googleDriveContent.documentId;
        }
        if (did) nbMap[did] = s.name;
      });
    }

    // --- STEP 3: Compute the delta ---
    // toDelete: sources in NotebookLM whose Drive ID no longer exists in staging
    //           (file was removed from source, so staging copy was also removed)
    // toUpload: files in staging that haven't been uploaded to NotebookLM yet
    const toDelete = Object.keys(nbMap).filter(did => !stagingMap[did]).map(did => nbMap[did]);
    let toUpload   = Object.values(stagingMap).filter(f => !nbMap[f.id]);

    // --- STEP 4: The Bouncer — filter out unsupported file types before upload ---
    // Even though Worker A already filters by MIME type at scan time, this is
    // a second safety check in case any unsupported files slipped into staging.
    toUpload = toUpload.filter(f => {
      let isSkipped = false;
      let reason    = "";

      if (!ALLOWED_MIME_TYPES.includes(f.mimeType)) {
        isSkipped = true;
        reason    = `Unsupported Type (${f.mimeType})`;
      } else if (f.mimeType === "application/vnd.google-apps.spreadsheet") {
        isSkipped = true;
        reason    = "Google Sheet (Not Supported)";
      }

      if (isSkipped) {
        console.warn(`🚫 [SKIP] ${f.name}`);
        localStats.skipped++;
        localFailures.push({ name: f.name, url: `https://drive.google.com/open?id=${f.id}`, reason: `SKIPPED: ${reason}` });
        return false;
      }
      return true;
    });

    console.log(`📊 [DELTA] To Delete: ${toDelete.length} | To Upload: ${toUpload.length}`);

    // --- STEP 5: Deletions ---
    // Remove stale sources from NotebookLM in batches of 50 (API limit).
    if (toDelete.length > 0) {
      for (let i = 0; i < toDelete.length; i += 50) {
        if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) break;
        try {
          deleteBatch(API_ENDPOINT, toDelete.slice(i, i + 50));
          localStats.deleted += toDelete.slice(i, i + 50).length;
        } catch(e) {
          localErrors.push({ action: "DELETE_BATCH", file: "Batch " + i, error: e.toString() });
        }
      }
    }

    // --- STEP 6: Uploads ---
    // Upload new files in small batches (UPLOAD_BATCH_SIZE = 2) with a 3-second
    // pause between batches to avoid overwhelming the API and triggering rate limits.
    if (toUpload.length > 0) {
      console.log(`🚀 [UPLOAD] Starting Batched Uploads...`);
      for (let i = 0; i < toUpload.length; i += UPLOAD_BATCH_SIZE) {
        if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) break;

        const batch = toUpload.slice(i, i + UPLOAD_BATCH_SIZE);

        // The NotebookLM API requires explicit MIME type for video files.
        batch.forEach(f => { if (f.name.toLowerCase().endsWith(".mp4")) f.mimeType = "video/mp4"; });

        try {
          uploadFilesBatch(API_ENDPOINT, batch);
          localStats.uploaded += batch.length;
          Utilities.sleep(3000); // Brief pause between batches to respect API rate limits
        } catch (e) {
          console.error(`❌ [BATCH FAIL] ${e}`);
          localErrors.push({ action: "UPLOAD_BATCH", file: "Batch", error: e.toString() });
          batch.forEach(f => {
            localFailures.push({ name: f.name, url: `https://drive.google.com/open?id=${f.id}`, reason: `UPLOAD ERR: ${e.toString().substring(0, 50)}` });
          });
        }
      }
    }

  } catch (e) {
    console.error("☠️ [CRITICAL WORKER B CRASH]", e);
    localErrors.push({ action: "CRASH", file: "Worker B", error: e.toString() });
  } finally {
    lock.releaseLock();
    sendDecoupledEmail("Worker B (Sync)", localStats, localFailures, localErrors);
    console.log("=== ✅ WORKER B FINISHED ===");
  }
}


// ==========================================
// HELPERS
// ==========================================

/**
 * Sends a nightly execution report email for the given worker.
 * Each worker calls this independently with its own stats and logs,
 * so the two email reports are fully decoupled and never mixed.
 */
function sendDecoupledEmail(workerName, stats, failures, errors) {
  const subject = `[Script Log] ${workerName} Report`;

  let body = `Execution Report for ${workerName}\n`;
  body    += `----------------------------------\n`;

  if (workerName.includes("Worker A")) {
    body += `Scanned:   ${stats.processed}\n`;
    body += `Copied:    ${stats.copied}\n`;
    body += `Renamed:   ${stats.renamed}\n`;
    body += `Skipped:   ${stats.skipped}\n`;
  } else {
    body += `Uploaded:  ${stats.uploaded}\n`;
    body += `Deleted:   ${stats.deleted}\n`;
    body += `Skipped:   ${stats.skipped}\n`;
  }
  body += `----------------------------------\n\n`;

  if (failures.length > 0) {
    body += `⚠️ FILES FAILED/SKIPPED (${failures.length}):\n`;
    body += `----------------------------------\n`;
    failures.forEach(f => {
      body += `📄 ${f.name}\n   Reason: ${f.reason}\n   Link: ${f.url}\n\n`;
    });
    body += `----------------------------------\n\n`;
  }

  if (errors.length > 0) {
    body += `🚨 SYSTEM ERRORS:\n`;
    errors.forEach(e => body += `[${e.action}] ${e.error}\n`);
  }

  if (failures.length === 0 && errors.length === 0) {
    body += `✅ Success. No errors or skipped files.\n`;
  }

  if (!RECIPIENT_EMAIL || RECIPIENT_EMAIL.includes("YOUR_EMAIL")) return;
  MailApp.sendEmail(RECIPIENT_EMAIL, subject, body);
}

/**
 * Calls the Gemini API to generate a clean, standardized source name in the
 * format "[Category] - [Title]". The result is cached in PropertiesService
 * immediately after generation to avoid repeat API calls for the same file.
 *
 * Falls back to cleanNameLogic() if:
 *  - No API key is configured
 *  - The API call fails or returns a non-200 response
 */
function callGeminiNaming(originalName, category, sourceId) {
  if (!GEMINI_API_KEY || GEMINI_API_KEY.includes("PASTE")) return cleanNameLogic(originalName, category);

  // Budget: total allowed length minus the category prefix and separator (" - ")
  const budget = TOTAL_LENGTH_LIMIT - category.length - 3;
  const prompt = `Task: Rename file. Category="${category}", Original="${originalName}". Rules: Format="${category} - [Title]". [Title] max ${budget} chars. Remove "go/" links. Keep dates.`;

  try {
    const res = UrlFetchApp.fetch(
      `https://generativelanguage.googleapis.com/${GEMINI_VERSION}/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
      { method: 'post', contentType: 'application/json', muteHttpExceptions: true,
        payload: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }) }
    );
    if (res.getResponseCode() !== 200) return cleanNameLogic(originalName, category);

    let text = JSON.parse(res.getContentText()).candidates[0].content.parts[0].text.trim();

    // Ensure the category prefix is present — the model occasionally omits it.
    if (!text.startsWith(category)) text = `${category} - ${text}`;

    // Truncate if the result still exceeds the character limit.
    if (text.length > TOTAL_LENGTH_LIMIT) text = text.substring(0, TOTAL_LENGTH_LIMIT - 3) + "...";

    // Persist to cache so subsequent runs don't call the API again for this file.
    if (sourceId) try { PropertiesService.getScriptProperties().setProperty(sourceId, text); } catch(e) {}

    return text;
  } catch (e) { return cleanNameLogic(originalName, category); }
}

/**
 * Recursively scans a Drive folder and its immediate subfolders.
 * - Resolves shortcuts to their target files
 * - Skips unsupported MIME types
 * - Uses the subfolder name as the "Category" for smart naming
 *
 * Only goes one level deep by design — deeper nesting is flattened
 * using the top-level subfolder name as the category.
 */
function scanFolderRecursive(folder, currentCategory, results) {
  const files = folder.getFiles();
  while (files.hasNext()) {
    const f = files.next();
    let targetFile = f, targetId = f.getId(), realMime = f.getMimeType();

    // Resolve Drive shortcuts — get the actual target file and its real MIME type.
    if (realMime === "application/vnd.google-apps.shortcut") {
      try {
        targetId   = f.getTargetId();
        targetFile = DriveApp.getFileById(targetId);
        realMime   = targetFile.getMimeType();
      } catch (e) { continue; } // Skip broken shortcuts
    }

    if (!ALLOWED_MIME_TYPES.includes(realMime)) continue;

    // Store the source file's metadata. Note: we use f.getId() (the shortcut's ID
    // if applicable) as the key so staging lookups remain consistent.
    results[f.getId()] = {
      targetId:     targetId,
      originalName: f.getName(),
      category:     currentCategory,   // null for files in the root folder
      modified:     targetFile.getLastUpdated()
    };
  }

  // Recurse into subfolders, passing the subfolder name as the category.
  const subs = folder.getFolders();
  while (subs.hasNext()) {
    const s = subs.next();
    scanFolderRecursive(s, s.getName(), results);
  }
}

/**
 * Basic name cleaner used as a fallback when Gemini is unavailable.
 * Strips the file extension, removes noise phrases, and prepends the category.
 * Truncates to TOTAL_LENGTH_LIMIT if needed.
 */
function cleanNameLogic(originalName, category) {
  let name = originalName.split('.')[0].replace(/copy of|internal only/gi, "").trim();
  let full = category ? `${category} - ${name}` : name;
  if (full.length > TOTAL_LENGTH_LIMIT) return full.substring(0, TOTAL_LENGTH_LIMIT - 3) + "...";
  return full;
}

/**
 * Returns the OAuth2 authorization headers required by the NotebookLM
 * Enterprise API. Uses the Apps Script service account token automatically.
 */
function getAuthHeaders() {
  return { "Authorization": "Bearer " + ScriptApp.getOAuthToken(), "Content-Type": "application/json" };
}

/**
 * Fetches the full list of sources currently in the NotebookLM notebook.
 * Throws on failure so Worker B can catch and log the error centrally.
 */
function getNotebookSources(ep) {
  try {
    const res = UrlFetchApp.fetch(ep, { method: "get", headers: getAuthHeaders(), muteHttpExceptions: true });
    if (res.getResponseCode() !== 200) throw new Error("NotebookLM Read Failed");
    return JSON.parse(res.getContentText()).sources || [];
  } catch(e) { throw e; }
}

/**
 * Deletes a batch of sources from NotebookLM by name.
 * The API accepts up to 50 names per call.
 */
function deleteBatch(ep, names) {
  UrlFetchApp.fetch(`${ep}/sources:batchDelete`, {
    method: 'post', headers: getAuthHeaders(), payload: JSON.stringify({ names })
  });
}

/**
 * Uploads a batch of Drive files to NotebookLM as sources.
 * Each file is referenced by its Drive document ID and display name.
 */
function uploadFilesBatch(ep, filesArray) {
  const contentList = filesArray.map(d => ({
    googleDriveContent: { documentId: d.id, sourceName: d.name, mimeType: d.mimeType }
  }));
  UrlFetchApp.fetch(`${ep}/sources:batchCreate`, {
    method: 'post', headers: getAuthHeaders(), payload: JSON.stringify({ userContents: contentList })
  });
}

/**
 * Utility function to manually clear the Gemini name cache.
 * Run this from the Apps Script editor if you want to force all files
 * to be re-named from scratch on the next Worker A run.
 */
function clearCache() {
  PropertiesService.getScriptProperties().deleteAllProperties();
  console.log("✅ CACHE CLEARED.");
}
