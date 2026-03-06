// assets/clientside.js
window.dash_clientside = Object.assign({}, window.dash_clientside, {
  utils: {
    // Focus the "Add Empty Rows" count input after clicking the button
    focusAddEmptyCount: function (n_clicks) {
      if (n_clicks) {
        setTimeout(function () {
          const input = document.getElementById("crud-add-empty-count");
          if (input) {
            input.focus();
            input.select();
          }
        }, 100);
      }
      return "";
    },

    // Paste from Excel (TSV/CSV) into structured rows
    // Returns an object: {rows: [...], error: null|string}
    pasteFromClipboard: async function (n_clicks, schemaCols) {
      if (!n_clicks) {
        return window.dash_clientside.no_update;
      }
      // ensure we have the schema
      if (!Array.isArray(schemaCols) || schemaCols.length === 0) {
        return { rows: [], error: "No schema loaded. Select a table first." };
      }
      try {
        const text = await navigator.clipboard.readText();
        if (!text) {
          return { rows: [], error: "Clipboard is empty." };
        }

        // Excel typically uses tab-separated values for clipboard
        const sep = text.indexOf("\t") !== -1 ? "\t" : ",";
        const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
        if (lines.length === 0) {
          return { rows: [], error: "No rows found in clipboard." };
        }

        // Detect header
        const firstCells = lines[0].split(sep).map(s => s.trim());
        const hasHeader = firstCells.length > 0 && firstCells.every(h => schemaCols.includes(h));
        let headers = [];
        let start = 0;

        if (hasHeader) {
          headers = firstCells;
          start = 1;
        } else {
          // map to the first N schema columns
          headers = schemaCols.slice(0, firstCells.length);
        }

        const outRows = [];
        for (let i = start; i < lines.length; i++) {
          const cells = lines[i].split(sep);
          // skip blank lines
          if (cells.length === 1 && cells[0].trim() === "") continue;

          const r = {};
          headers.forEach((h, j) => {
            r[h] = (cells[j] !== undefined ? cells[j] : "");
          });

          // internal markers used by your save logic
          const rid = (window.crypto && crypto.randomUUID)
            ? crypto.randomUUID()
            : Math.random().toString(36).slice(2);
          r["_row_id"] = rid;
          r["_new"] = true;

          outRows.push(r);
        }

        return { rows: outRows, error: null };
      } catch (e) {
        return { rows: [], error: (e && e.message) ? e.message : "Clipboard read failed." };
      }
    }
  }
});
