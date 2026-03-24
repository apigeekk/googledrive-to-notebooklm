// ==========================================
//      ENTERPRISE KNOWLEDGE BASE SYNC
//      (Production v1.4 - Fully Decoupled Emails)
// ==========================================

// --- CONFIGURATION ---
const GCP_PROJECT_NUMBER = "GCP_PROJECT_NUMBER";
const NOTEBOOK_ID = "NOTEBOOK_ID"; 
const SOURCE_MASTER_ID = "SOURCE_MASTER_ID"; 
const STAGING_FOLDER_ID = "STAGING_FOLDER_ID"; 
const GEMINI_API_KEY = "GEMINI_API_KEY"; 
const RECIPIENT_EMAIL = "RECIPIENT_EMAIL"; 

// AI Configuration
const GEMINI_MODEL = "gemini-3-flash-preview"; 
const GEMINI_VERSION = "v1beta"; 

// System Limits
const LOCATION = "global";
const TOTAL_LENGTH_LIMIT = 65; 
const MAX_RUNTIME_MS = 25 * 60 * 1000; 
const UPLOAD_BATCH_SIZE = 2; 

const ALLOWED_MIME_TYPES = [
  "application/vnd.google-apps.document",      
  "application/vnd.google-apps.presentation",  
  "application/pdf",                           
  "text/plain",                                
  "text/markdown",                             
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",   
  "application/vnd.openxmlformats-officedocument.presentationml.presentation", 
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",         
  "audio/mpeg",  "audio/wav",   "audio/mp4",   "video/mp4",   "audio/x-m4a"
];

// ==========================================
// WORKER A: THE ARCHITECT (1:00 AM)
// ==========================================

function runReplicatorOnly() {
  console.log("=== 🏗️ WORKER A STARTED: REPLICATOR ===");
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) { console.warn("🔒 LOCKED: Script is already running."); return; }

  const startTime = new Date().getTime();
  
  // 🟢 LOCAL STATS (Unique to this run)
  let localStats = { processed: 0, copied: 0, renamed: 0, skipped: 0 };
  let localFailures = []; // Stores copy/rename errors
  let localErrors = [];   // Stores critical crashes

  try {
    const sourceFolder = DriveApp.getFolderById(SOURCE_MASTER_ID);
    const stagingFolder = DriveApp.getFolderById(STAGING_FOLDER_ID);
    
    // 1. Load Cache
    console.log("🔍 [INIT] Loading Name Cache...");
    const scriptProperties = PropertiesService.getScriptProperties();
    const nameCache = scriptProperties.getProperties(); 

    // 2. Map Staging Files
    const stagingFiles = {};
    const sIter = stagingFolder.getFiles();
    while (sIter.hasNext()) {
      const f = sIter.next();
      const desc = f.getDescription(); 
      if (desc && desc.startsWith("source:")) {
        stagingFiles[desc.split(":")[1]] = f;
      }
    }

    // 3. Scan Source Folder
    const sourceFiles = {};
    scanFolderRecursive(sourceFolder, null, sourceFiles);
    const sourceIds = Object.keys(sourceFiles);
    const totalFiles = sourceIds.length;
    console.log(`✅ [INIT] Found ${totalFiles} source files.`);

    // 4. The Loop
    for (let i = 0; i < totalFiles; i++) {
      const id = sourceIds[i];
      const progressTag = `[${i+1}/${totalFiles}]`; 

      if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) {
        console.warn(`⚠️ [TIMEOUT] Stopping Worker A.`);
        localErrors.push({ action: "TIMEOUT", file: "System", error: "25min limit reached" });
        break; 
      }

      localStats.processed++;
      const data = sourceFiles[id];
      let needsCopy = false;
      let needsRename = false;

      // Check
      if (stagingFiles[id]) {
        const stagedFile = stagingFiles[id];
        const sourceTime = new Date(data.modified).getTime();
        const stagedTime = new Date(stagedFile.getLastUpdated()).getTime();

        if (sourceTime > stagedTime + 60000) {
          needsCopy = true;
          try { stagedFile.setTrashed(true); } catch(e){}
        } 
        else if (!stagedFile.getName().startsWith(`${data.category} - `)) {
          needsRename = true;
        } else {
          localStats.skipped++;
        }
      } else {
        needsCopy = true;
      }

      if (!needsCopy && !needsRename) continue; 

      // Naming
      let smartName;
      if (nameCache[id]) {
          smartName = nameCache[id];
          if (!smartName.startsWith(data.category)) {
             let pureTitle = smartName.includes(" - ") ? smartName.split(" - ").slice(1).join(" - ") : smartName;
             smartName = `${data.category} - ${pureTitle}`;
          }
      } else {
          if (data.category) {
              smartName = callGeminiNaming(data.originalName, data.category, id);
              nameCache[id] = smartName; 
          } else {
              smartName = cleanNameLogic(data.originalName, null);
          }
      }

      // Execute
      if (needsCopy) {
        console.log(`${progressTag} 📂 [COPY] -> ${smartName}`);
        try {
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
    // 📨 SEND EMAIL ONLY FOR WORKER A
    sendDecoupledEmail("Worker A (Replicator)", localStats, localFailures, localErrors);
    console.log("=== ✅ WORKER A FINISHED ===");
  }
}

// ==========================================
// WORKER B: THE MOVER (2:00 AM)
// ==========================================

function runSyncOnly() {
  console.log("=== 🚚 WORKER B STARTED: SYNC ===");
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1000)) { console.warn("🔒 LOCKED: Script is already running."); return; }
  
  const startTime = new Date().getTime();
  const API_ENDPOINT = `https://${LOCATION}-discoveryengine.googleapis.com/v1alpha/projects/${GCP_PROJECT_NUMBER}/locations/${LOCATION}/notebooks/${NOTEBOOK_ID}`;

  // 🟢 LOCAL STATS (Unique to this run)
  let localStats = { uploaded: 0, deleted: 0, skipped: 0 };
  let localFailures = []; // Stores upload/bouncer errors
  let localErrors = [];   // Stores critical crashes

  try {
    // 1. Get Local Files
    console.log("🔍 [SYNC] Scanning Staging...");
    const stagingMap = {};
    const folder = DriveApp.getFolderById(STAGING_FOLDER_ID);
    const files = folder.getFiles();
    while (files.hasNext()) {
      const f = files.next();
      stagingMap[f.getId()] = { id: f.getId(), name: f.getName(), mimeType: f.getMimeType() };
    }

    // 2. Get Remote Files
    console.log("🔍 [SYNC] Fetching NotebookLM...");
    const currentSources = getNotebookSources(API_ENDPOINT);
    const nbMap = {}; 
    if (currentSources && currentSources.length > 0) {
      currentSources.forEach(s => {
        let did = null;
        if (s.metadata && s.metadata.googleDocsMetadata) {
            did = s.metadata.googleDocsMetadata.documentId;
        } else if (s.googleDriveContent) {
            did = s.googleDriveContent.documentId;
        }
        if (did) nbMap[did] = s.name;
      });
    }

    // 3. Deltas
    const toDelete = Object.keys(nbMap).filter(did => !stagingMap[did]).map(did => nbMap[did]);
    let toUpload = Object.values(stagingMap).filter(f => !nbMap[f.id]);

    // 4.5 The Bouncer
    toUpload = toUpload.filter(f => {
      let isSkipped = false;
      let reason = "";

      if (!ALLOWED_MIME_TYPES.includes(f.mimeType)) { isSkipped = true; reason = `Unsupported Type (${f.mimeType})`; }
      else if (f.mimeType === "application/vnd.google-apps.spreadsheet") { isSkipped = true; reason = "Google Sheet (Not Supported)"; }

      if (isSkipped) {
        console.warn(`🚫 [SKIP] ${f.name}`);
        localStats.skipped++;
        localFailures.push({ name: f.name, url: `https://drive.google.com/open?id=${f.id}`, reason: `SKIPPED: ${reason}` });
        return false;
      }
      return true;
    });
    
    console.log(`📊 [DELTA] To Delete: ${toDelete.length} | To Upload: ${toUpload.length}`);

    // 4. Deletions
    if (toDelete.length > 0) {
      for (let i = 0; i < toDelete.length; i += 50) {
        if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) break;
        try {
          deleteBatch(API_ENDPOINT, toDelete.slice(i, i+50));
          localStats.deleted += toDelete.slice(i, i+50).length;
        } catch(e) {
           localErrors.push({ action: "DELETE_BATCH", file: "Batch " + i, error: e.toString() });
        }
      }
    }

    // 5. Uploads (Batched)
    if (toUpload.length > 0) {
      console.log(`🚀 [UPLOAD] Starting Batched Uploads...`);
      for (let i = 0; i < toUpload.length; i += UPLOAD_BATCH_SIZE) {
        if ((new Date().getTime() - startTime) > MAX_RUNTIME_MS) break;

        const batch = toUpload.slice(i, i + UPLOAD_BATCH_SIZE);
        batch.forEach(f => { if (f.name.toLowerCase().endsWith(".mp4")) f.mimeType = "video/mp4"; });

        try {
          uploadFilesBatch(API_ENDPOINT, batch);
          localStats.uploaded += batch.length;
          Utilities.sleep(3000); 
        } catch (e) {
          console.error(`❌ [BATCH FAIL] ${e}`);
          localErrors.push({ action: "UPLOAD_BATCH", file: "Batch", error: e.toString() });
          batch.forEach(f => {
             localFailures.push({ name: f.name, url: `https://drive.google.com/open?id=${f.id}`, reason: `UPLOAD ERR: ${e.toString().substring(0,50)}` });
          });
        }
      }
    }

  } catch (e) {
    console.error("☠️ [CRITICAL WORKER B CRASH]", e);
    localErrors.push({ action: "CRASH", file: "Worker B", error: e.toString() });
  } finally {
    lock.releaseLock();
    // 📨 SEND EMAIL ONLY FOR WORKER B
    sendDecoupledEmail("Worker B (Sync)", localStats, localFailures, localErrors);
    console.log("=== ✅ WORKER B FINISHED ===");
  }
}

// ==========================================
// HELPERS
// ==========================================

// ✅ UPDATED: Accepts stats/logs as arguments
function sendDecoupledEmail(workerName, stats, failures, errors) {
  const subject = `[Script Log] ${workerName} Report`;
  
  let body = `Execution Report for ${workerName}\n`;
  body += `----------------------------------\n`;
  
  // Dynamic Stats display based on Worker type
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

function callGeminiNaming(originalName, category, sourceId) {
  if (!GEMINI_API_KEY || GEMINI_API_KEY.includes("PASTE")) return cleanNameLogic(originalName, category);
  const budget = TOTAL_LENGTH_LIMIT - category.length - 3;
  const prompt = `Task: Rename file. Category="${category}", Original="${originalName}". Rules: Format="${category} - [Title]". [Title] max ${budget} chars. Remove "go/" links. Keep dates.`;
  try {
    const res = UrlFetchApp.fetch(`https://generativelanguage.googleapis.com/${GEMINI_VERSION}/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`, {
      method: 'post', contentType: 'application/json', muteHttpExceptions: true,
      payload: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
    });
    if (res.getResponseCode() !== 200) return cleanNameLogic(originalName, category);
    let text = JSON.parse(res.getContentText()).candidates[0].content.parts[0].text.trim();
    if (!text.startsWith(category)) text = `${category} - ${text}`;
    if (text.length > TOTAL_LENGTH_LIMIT) text = text.substring(0, TOTAL_LENGTH_LIMIT - 3) + "...";
    if (sourceId) try { PropertiesService.getScriptProperties().setProperty(sourceId, text); } catch(e) {}
    return text;
  } catch (e) { return cleanNameLogic(originalName, category); }
}

function scanFolderRecursive(folder, currentCategory, results) {
  const files = folder.getFiles();
  while (files.hasNext()) {
    const f = files.next();
    let targetFile = f, targetId = f.getId(), realMime = f.getMimeType();
    if (realMime === "application/vnd.google-apps.shortcut") {
      try { targetId = f.getTargetId(); targetFile = DriveApp.getFileById(targetId); realMime = targetFile.getMimeType(); } catch (e) { continue; } 
    }
    if (!ALLOWED_MIME_TYPES.includes(realMime)) continue;
    results[f.getId()] = { targetId: targetId, originalName: f.getName(), category: currentCategory, modified: targetFile.getLastUpdated() };
  }
  const subs = folder.getFolders();
  while (subs.hasNext()) { const s = subs.next(); scanFolderRecursive(s, s.getName(), results); }
}

function cleanNameLogic(originalName, category) {
  let name = originalName.split('.')[0].replace(/copy of|internal only/gi, "").trim();
  let full = category ? `${category} - ${name}` : name;
  if (full.length > TOTAL_LENGTH_LIMIT) return full.substring(0, TOTAL_LENGTH_LIMIT - 3) + "...";
  return full;
}

function getAuthHeaders() { return { "Authorization": "Bearer " + ScriptApp.getOAuthToken(), "Content-Type": "application/json" }; }

function getNotebookSources(ep) { 
  try { 
    const res = UrlFetchApp.fetch(ep, { method: "get", headers: getAuthHeaders(), muteHttpExceptions: true }); 
    if (res.getResponseCode() !== 200) throw new Error("NotebookLM Read Failed");
    return JSON.parse(res.getContentText()).sources || [];
  } catch(e) { throw e; } 
}

function deleteBatch(ep, names) { 
  UrlFetchApp.fetch(`${ep}/sources:batchDelete`, { method: 'post', headers: getAuthHeaders(), payload: JSON.stringify({names}) }); 
}

function uploadFilesBatch(ep, filesArray) { 
  const contentList = filesArray.map(d => ({ googleDriveContent: { documentId: d.id, sourceName: d.name, mimeType: d.mimeType } }));
  UrlFetchApp.fetch(`${ep}/sources:batchCreate`, { method: 'post', headers: getAuthHeaders(), payload: JSON.stringify({ userContents: contentList }) }); 
}

function clearCache() {
  PropertiesService.getScriptProperties().deleteAllProperties();
  console.log("✅ CACHE CLEARED.");
}
