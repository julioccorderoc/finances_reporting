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

  /* Everything a Tab can land on, minus the things it must not: a
   * disabled arrow at the end of the run, and a control inside a
   * disclosure that is merely x-shown (the picker's full list is in the
   * DOM the whole time). `[role="dialog"]` itself carries tabindex="-1"
   * so it can take focus on open without joining the cycle. */
  var FOCUSABLE = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    '[tabindex]:not([tabindex="-1"])',
  ].join(",");

  function focusable(root) {
    return Array.prototype.filter.call(
      root.querySelectorAll(FOCUSABLE),
      function (el) {
        /* getClientRects rather than offsetParent: it reports empty for
         * display:none AND for a zero-box element, and unlike
         * offsetParent it does not lie about position:fixed. */
        return el.getClientRects().length > 0;
      }
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
      bulkCatKind: null,
      /* J2: which row the run was started from, so closing puts the
       * keyboard back where it found it. An ITEM ID, not an element —
       * closing refreshes the queue, so the node that was clicked is
       * detached by the time focus is restored. Lives here rather than
       * in the modal because the modal is torn down and rebuilt on
       * every advance. */
      focusOrigin: null,
      /* J5: what the persistent live region says. Written from the
       * queue's own rendered headline after each swap, so the sentence
       * a screen reader hears and the sentence on screen cannot drift. */
      announcement: "",

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

      /* -- focus and announcements (J2, J5) ----------------------- */
      rememberOrigin: function (id) {
        this.openId = id;
        this.focusOrigin = id;
      },
      /* Called on close-modal. The row may still be there (the owner
       * looked and left), or gone (they resolved it and the queue
       * re-rendered) — so it lands twice: once now, and once after the
       * refresh settles, whichever of the two finds the row.
       *
       * Order of preference: the row's own open button, the run button,
       * the queue itself. Focus is never left on <body>. */
      restoreFocus: function () {
        var id = this.focusOrigin;
        this.focusOrigin = null;

        var land = function () {
          var row = id ? rowFor(id) : null;
          var target =
            (row && row.querySelector(".triage-row-open")) ||
            document.querySelector("[data-sort-all]") ||
            document.getElementById("triage-queue");
          if (!target) return;
          /* The queue is a plain container; make it focusable for the
           * one moment it has to be. */
          if (target.id === "triage-queue" && !target.hasAttribute("tabindex")) {
            target.setAttribute("tabindex", "-1");
          }
          target.focus();
        };

        land();

        var host = document.getElementById("triage-queue");
        if (!host) return;
        var settle = function () {
          host.removeEventListener("htmx:afterSettle", settle);
          land();
        };
        host.addEventListener("htmx:afterSettle", settle);
        /* A close with nothing to reconcile never swaps, so the listener
         * would sit there forever waiting for a settle that is not
         * coming (C7's rule, applied to this one too). */
        window.setTimeout(function () {
          host.removeEventListener("htmx:afterSettle", settle);
        }, 1200);
      },
      /* The headline and the three badges, read back off the queue that
       * just rendered. Repeating the server's own words means there is
       * no second copy of the counting rules to drift. */
      announceCounts: function () {
        var host = document.getElementById("triage-queue");
        if (!host) return;
        var parts = [];
        var answer = host.querySelector(".triage-answer");
        if (answer) parts.push(answer.textContent.trim());
        Array.prototype.forEach.call(
          host.querySelectorAll(".triage-meta .tbadge"),
          function (badge) {
            parts.push(badge.textContent.trim());
          }
        );
        this.announcement = parts.join(", ");
      },

      /* -- sheets ------------------------------------------------- */
      closeSheet: function () {
        var host = document.getElementById("triage-sheet-host");
        if (host) host.replaceChildren();
        this.bulkCat = null;
        this.bulkCatLabel = null;
        this.bulkCatKind = null;
      },
      /* The bulk sheet's picker writes here; the modal's picker shadows
       * these two with its own per-row versions. */
      pickedCategory: function () {
        return this.bulkCat;
      },
      pickCategory: function (id, label, kind) {
        this.bulkCat = id;
        this.bulkCatLabel = label;
        this.bulkCatKind = kind || null;
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
        var kind = this.bulkCatKind;
        return this.selected
          .map(rowFor)
          .filter(function (el) {
            if (!el || !el.dataset.txnId) return false;
            if (el.dataset.needsCategory !== "true") return false;
            /* And a kind the category can legally land on. The bulk
             * endpoint writes through the repo rather than apply_edit, so
             * it would happily file an income row under Fees; the count
             * and the write agree here instead. A transfer-kind category
             * is "moved, not spent" and fits any income or expense row
             * (category_fits is asymmetric on purpose), so it never
             * narrows the set. */
            return !kind || kind === "transfer" || el.dataset.kind === kind;
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

  /* One entry of the run. Its state is READ from and WRITTEN to the page
   * scope's `drafts`, keyed by item id, so walking away to another entry
   * and back finds the half-made decision still there (B13) — this
   * component itself is destroyed and rebuilt on every swap. */
  window.triageModal = function triageModal() {
    return {
      itemId: null,
      needsCat: false,
      needsRate: false,
      needsPair: false,
      amountNative: null,
      currency: "",
      currentUsd: null,

      adopt: function () {
        var d = this.$el.dataset;
        this.itemId = d.itemId;
        this.needsCat = d.needsCat === "true";
        this.needsRate = d.needsRate === "true";
        this.needsPair = d.needsPair === "true";
        this.amountNative = d.amountNative === "" ? null : Number(d.amountNative);
        this.currency = d.currency || "";
        this.currentUsd = d.currentUsd === "" ? null : Number(d.currentUsd);
        /* The list highlights whichever row the run is on, including the
         * ones it advanced into rather than opened by hand. */
        this.openId = this.itemId;
        /* Advancing INTO a dialog replaces the element that had focus, so
         * it has to claim focus itself or the keyboard is left on
         * <body> — from where the first Tab walks into the queue behind
         * the scrim. `trapTab` keeps it here from then on (J2). */
        var dialog = this.dialog();
        if (dialog) dialog.focus();
        /* Opening by URL rather than from a row leaves nothing to
         * return to; `restoreFocus` falls back to the run button. */
        if (this.focusOrigin === null) this.focusOrigin = this.itemId;
      },

      dialog: function () {
        return this.$el.querySelector('[role="dialog"]');
      },

      /* J2 — the trap itself. Plain JS, no library: Tab from the last
       * control wraps to the first, Shift-Tab from the first wraps to
       * the last, and focus that has escaped the dialog altogether (a
       * browser that restored it elsewhere, or the dialog itself, which
       * is tabindex="-1" and outside the cycle) is pulled back to an
       * end. */
      trapTab: function (event) {
        var dialog = this.dialog();
        if (!dialog) return;

        var items = focusable(dialog);
        if (!items.length) {
          event.preventDefault();
          dialog.focus();
          return;
        }

        var first = items[0];
        var last = items[items.length - 1];
        var active = document.activeElement;

        if (!dialog.contains(active) || active === dialog) {
          event.preventDefault();
          if (event.shiftKey) {
            last.focus();
          } else {
            first.focus();
          }
          return;
        }
        if (event.shiftKey && active === first) {
          event.preventDefault();
          last.focus();
          return;
        }
        if (!event.shiftKey && active === last) {
          event.preventDefault();
          first.focus();
        }
      },

      draftHere: function () {
        return this.draft(this.itemId);
      },
      catId: function () {
        var v = this.draftHere().cat;
        return v === undefined ? null : v;
      },
      setCat: function (id, label) {
        this.setDraft(this.itemId, { cat: id, catLabel: label });
      },
      /* Shadows the page scope's pair, so the picker inside this dialog
       * writes into THIS row's draft rather than into the bulk sheet's
       * pending category. */
      pickedCategory: function () {
        return this.catId();
      },
      pickCategory: function (id, label) {
        this.setCat(id, label);
      },
      rateValue: function () {
        return this.draftHere().rate || "";
      },
      setRate: function (value) {
        this.setDraft(this.itemId, { rate: value });
      },
      noteValue: function () {
        return this.draftHere().note || "";
      },
      setNote: function (value) {
        this.setDraft(this.itemId, { note: value });
      },

      /* "Resolvable": a row that needs a category has one, a row that only
       * needs a rate has a rate above zero. An approximate rate never
       * blocks (D6), so a category row is ready without one. */
      ready: function () {
        if (this.needsPair) return false;
        if (this.needsCat) return this.catId() !== null;
        if (this.needsRate) return Number(this.rateValue()) > 0;
        return true;
      },
      previewUsd: function () {
        var rate = Number(this.rateValue());
        if (rate > 0 && this.amountNative !== null) {
          return usd(this.amountNative / rate, true);
        }
        return usd(this.currentUsd, true);
      },

      submit: function () {
        /* ↵ goes through the form itself, exactly as the footer button
         * does. requestSubmit() fires the submit event htmx listens for;
         * form.submit() would bypass it and navigate the page away. */
        if (!this.ready()) return;
        var form = this.$el.querySelector("form[data-triage-form]");
        if (form) form.requestSubmit();
      },
      close: function () {
        this.openId = null;
        window.dispatchEvent(new CustomEvent("close-modal"));
      },
      step: function (which) {
        /* Click the arrow rather than re-deriving its URL: one place
         * decides what each arrow points at, and the disabled state at
         * the ends is honoured for free (B6). */
        var el = this.$el.querySelector(
          "[data-nav-" + which + "]:not([disabled])"
        );
        if (el) el.click();
      },

      onKey: function (event) {
        /* esc closes from anywhere, including from inside a field (C4). */
        if (event.key === "Escape") {
          this.close();
          return;
        }
        /* Before the typing guard: Tab has to keep working inside a
         * field, or the trap has a hole exactly where the caret is. */
        if (event.key === "Tab") {
          this.trapTab(event);
          return;
        }
        var target = event.target;
        var typing =
          target && /^(input|textarea|select)$/i.test(target.tagName);
        /* Everything else is swallowed while a field has focus: typing "3"
         * into the rate box must not also pick a category (C5). */
        if (typing) return;
        /* Key repeat would submit each dialog the advance swaps in,
         * walking the queue and resolving rows nobody saw. */
        if (event.repeat) return;

        if (event.key === "ArrowLeft") {
          event.preventDefault();
          this.step("prev");
          return;
        }
        if (event.key === "ArrowRight") {
          event.preventDefault();
          this.step("next");
          return;
        }
        if (event.key === "Enter") {
          /* Only when resolvable — otherwise nothing happens at all: no
           * error flash, no submit (C3). */
          if (this.ready()) {
            event.preventDefault();
            this.submit();
          }
          return;
        }
        if (this.needsCat && event.key >= "1" && event.key <= "8") {
          var chip = this.$el.querySelector(
            '[data-chip="' + event.key + '"]'
          );
          if (chip) {
            chip.click();
            event.preventDefault();
          }
        }
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

  /* --- The rate chip's tooltip ------------------------------------------
   *
   * CSS alone cannot place this. The money cell is `min-width: 0;
   * overflow: hidden` — the grid-cell guard that stops a long figure
   * blowing out its column — so an absolutely-positioned bubble inside it
   * is clipped away: present in the DOM, `visibility: visible`, correct
   * bounding box, and not on screen. `position: fixed` escapes that, and
   * viewport coordinates are the one thing a stylesheet cannot supply.
   *
   * The bubble is `visibility: hidden`, never `display: none`, so it is
   * laid out at all times and can be measured before it is shown.
   *
   * Delegated on `document`, because every list on this surface is
   * replaced wholesale by htmx: a listener bound to the chips themselves
   * would be right on a cold load and gone for the rest of the sitting. */
  var GUTTER = 8;
  var activeProv = null;

  function placeProvHelp(chip) {
    var help = chip.querySelector(".prov-help");
    if (!help) return;
    var anchor = chip.getBoundingClientRect();
    var bubble = help.getBoundingClientRect();

    /* Right edges aligned, because the chip trails a right-aligned money
     * block nearly everywhere; clamped so neither edge leaves the window. */
    var left = Math.min(
      anchor.right - bubble.width,
      window.innerWidth - bubble.width - GUTTER
    );
    var top = anchor.bottom + 6;
    /* Flip above rather than hang off the bottom of the window. */
    if (top + bubble.height > window.innerHeight - GUTTER) {
      top = anchor.top - bubble.height - 6;
    }

    help.style.left = Math.max(GUTTER, left) + "px";
    help.style.top = Math.max(GUTTER, top) + "px";
  }

  function onProvActivate(event) {
    var target = event.target;
    var chip = target && target.closest ? target.closest(".prov") : null;
    if (!chip) return;
    activeProv = chip;
    placeProvHelp(chip);
  }

  document.addEventListener("mouseover", onProvActivate);
  document.addEventListener("focusin", onProvActivate);
  /* Capture phase: the queue scrolls in its own container, not the window,
   * and a fixed bubble left at stale coordinates detaches from its chip. */
  document.addEventListener(
    "scroll",
    function () {
      if (activeProv && activeProv.isConnected) placeProvHelp(activeProv);
    },
    true
  );

  /* Exposed for the modal, which formats its rate preview as the owner
   * types (D8) and its footer label from the same numbers. */
  window.triageFormat = { usd: usd, native: native, rate: rateStr };
})();
