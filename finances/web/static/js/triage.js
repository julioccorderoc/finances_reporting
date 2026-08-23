/* Triage — the browser half of the redesigned queue.
 *
 * Two Alpine components, both plain globals so they are defined before
 * Alpine's deferred script runs:
 *
 *   triageScreen()  the page scope: selection, collapse, the sitting
 *                   counter, the per-row drafts the modal run reads, and
 *                   the bulk writes.
 *   catPicker()     one category picker's local state: the query, the
 *                   disclosure, and which category's test is on show.
 *
 * Everything that touches the ledger goes through an endpoint. Nothing
 * here computes money except the rate preview, which is a preview.
 */

(function () {
  "use strict";

  /* U+2212, not the hyphen a keyboard makes. Mirrors finances.format so
   * the live "WOULD BECOME" figure and the server-rendered one beside it
   * cannot look like different kinds of number. */
  var MINUS = "−";
  var NBSP = " ";

  function group(value) {
    return Math.abs(value).toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  function usd(value, signed) {
    if (value === null || value === undefined || !isFinite(value)) return "—";
    var sign = value < 0 ? MINUS : signed && value > 0 ? "+" : "";
    return sign + "$" + group(value);
  }

  function native(value, currency, signed) {
    if (value === null || value === undefined || !isFinite(value)) return "—";
    var sign = value < 0 ? MINUS : signed && value > 0 ? "+" : "";
    var ticker = (currency || "").toUpperCase();
    if (ticker === "USDT" || ticker === "USDC") {
      return sign + group(value) + NBSP + ticker;
    }
    if (ticker === "VES") return sign + "Bs." + NBSP + group(value);
    return usd(value, signed);
  }

  function rateStr(value) {
    if (value === null || value === undefined || !isFinite(value)) return "";
    return group(value);
  }

  function rowFor(itemId) {
    return document.querySelector(
      '[data-item-id="' + (window.CSS ? CSS.escape(itemId) : itemId) + '"]'
    );
  }

  function toast(message, level) {
    window.dispatchEvent(
      new CustomEvent("show-toast", {
        detail: { level: level || "success", message: message },
      })
    );
  }

  window.triageScreen = function triageScreen() {
    return {
      /* The selection is a list of item ids, not of DOM nodes: a queue
       * swap replaces every row, and collapsing a group hides rows that
       * are still selected (G1). */
      selected: [],
      /* Starts with *Priced roughly* shut. Collapse is a reading
       * convenience — the run still walks every entry (B2). */
      collapsed: [2],
      done: 0,
      openId: null,
      /* Per-row drafts, so walking away from an entry and back does not
       * lose a half-made decision (B13). Keyed by item id. */
      drafts: {},
      bulkCat: null,
      bulkCatLabel: null,

      /* -- selection ---------------------------------------------- */
      isSelected: function (id) {
        return this.selected.indexOf(id) > -1;
      },
      toggleRow: function (id) {
        if (this.isSelected(id)) {
          this.selected = this.selected.filter(function (x) {
            return x !== id;
          });
        } else {
          this.selected = this.selected.concat(id);
        }
      },
      clearSelection: function () {
        this.selected = [];
      },
      rowClasses: function (id) {
        if (this.openId === id) return "is-open";
        return this.isSelected(id) ? "is-selected" : "";
      },
      groupAllSelected: function (ids) {
        var self = this;
        return (
          ids.length > 0 &&
          ids.every(function (id) {
            return self.isSelected(id);
          })
        );
      },
      toggleGroupSelection: function (ids) {
        var self = this;
        if (this.groupAllSelected(ids)) {
          this.selected = this.selected.filter(function (id) {
            return ids.indexOf(id) < 0;
          });
          return;
        }
        ids.forEach(function (id) {
          if (!self.isSelected(id)) self.selected = self.selected.concat(id);
        });
      },

      /* -- collapse ----------------------------------------------- */
      isCollapsed: function (bucket) {
        return this.collapsed.indexOf(bucket) > -1;
      },
      toggleGroup: function (bucket) {
        if (this.isCollapsed(bucket)) {
          this.collapsed = this.collapsed.filter(function (b) {
            return b !== bucket;
          });
        } else {
          this.collapsed = this.collapsed.concat(bucket);
        }
      },

      /* -- drafts ------------------------------------------------- */
      draft: function (id) {
        return this.drafts[id] || {};
      },
      setDraft: function (id, patch) {
        this.drafts[id] = Object.assign({}, this.drafts[id] || {}, patch);
      },

      /* -- sheets ------------------------------------------------- */
      closeSheet: function () {
        var host = document.getElementById("triage-sheet-host");
        if (host) host.replaceChildren();
        this.bulkCat = null;
        this.bulkCatLabel = null;
      },
      /* The bulk sheet's picker writes here; the modal's picker shadows
       * these two with its own per-row versions. */
      pickedCategory: function () {
        return this.bulkCat;
      },
      pickCategory: function (id, label) {
        this.bulkCat = id;
        this.bulkCatLabel = label;
      },

      /* -- bulk --------------------------------------------------- */
      selectedTxnIds: function () {
        return this.selected
          .map(rowFor)
          .filter(function (el) {
            return el && el.dataset.txnId;
          })
          .map(function (el) {
            return Number(el.dataset.txnId);
          });
      },
      /* G4: only the selected rows that ACTUALLY need a category. A row
       * that already has one is finished work, and re-filing it would
       * overwrite a decision the owner already made. */
      bulkTargets: function () {
        return this.selected
          .map(rowFor)
          .filter(function (el) {
            return (
              el && el.dataset.txnId && el.dataset.needsCategory === "true"
            );
          })
          .map(function (el) {
            return Number(el.dataset.txnId);
          });
      },
      refreshQueue: function () {
        htmx.ajax("GET", "/_partial/triage/queue", {
          target: "#triage-queue",
          swap: "innerHTML",
        });
      },
      applyBulkCategory: function () {
        var self = this;
        var ids = this.bulkTargets();
        var label = this.bulkCatLabel;
        var category = this.bulkCat;
        if (!ids.length || category === null) return;
        fetch("/api/transactions/bulk-edit", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ids: ids, category_id: category }),
        })
          .then(function (response) {
            if (!response.ok) throw new Error("HTTP " + response.status);
            return response.json();
          })
          .then(function (data) {
            /* Only now: a failed write must leave every row where it was
             * (K11), so nothing is removed until the server says so. */
            self.done += data.updated;
            self.clearSelection();
            self.closeSheet();
            self.refreshQueue();
            toast(data.updated + " rows sorted into " + label + ".");
          })
          .catch(function (err) {
            toast(
              "Nothing was sorted — the write failed (" + err.message + ").",
              "error"
            );
          });
      },
      parkSelected: function () {
        var ids = this.selectedTxnIds();
        if (!ids.length) return;
        htmx.ajax("POST", "/_partial/triage/bulk-park", {
          target: "#triage-queue",
          swap: "innerHTML",
          values: { ids: ids.join(",") },
        });
        this.clearSelection();
      },
    };
  };

  window.catPicker = function catPicker() {
    return {
      q: "",
      showAll: false,
      hoverLabel: null,
      hoverTest: null,

      /* E4: the haystack is "label test", lowercased server-side, so a
       * search for "prescription" finds Health by its ruling rather than
       * by its name. */
      matches: function (haystack) {
        var q = this.q.trim().toLowerCase();
        return !q || (haystack || "").indexOf(q) > -1;
      },
      noMatch: function () {
        var q = this.q.trim().toLowerCase();
        if (!q) return false;
        return !Array.prototype.some.call(
          this.$root.querySelectorAll("[data-search]"),
          function (el) {
            return el.dataset.search.indexOf(q) > -1;
          }
        );
      },
      hoverFrom: function (el) {
        this.hoverLabel = el.dataset.label;
        this.hoverTest = el.dataset.test;
      },
      hoverClear: function () {
        this.hoverLabel = null;
        this.hoverTest = null;
      },
      selectedEl: function () {
        var id = this.pickedCategory();
        if (id === null || id === undefined) return null;
        return this.$root.querySelector('[data-cat-id="' + id + '"]');
      },
      shownLabel: function () {
        if (this.hoverLabel) return this.hoverLabel;
        var el = this.selectedEl();
        return el ? el.dataset.label : "";
      },
      shownTest: function () {
        if (this.hoverTest) return this.hoverTest;
        var el = this.selectedEl();
        return el ? el.dataset.test : "";
      },
    };
  };

  /* Exposed for the modal, which formats its rate preview as the owner
   * types (D8) and its footer label from the same numbers. */
  window.triageFormat = { usd: usd, native: native, rate: rateStr };
})();
