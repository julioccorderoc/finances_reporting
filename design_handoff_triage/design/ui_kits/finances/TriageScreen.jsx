// Triage — the chosen shape: one dense list you sweep. Rows are grouped by what
// is wrong with them, resolvable in place, and the guess can be accepted from the
// row itself. A rate nothing could price is filled in with the nearest one and
// flagged, so it never blocks a sitting.
window.Triage = window.Triage || {};

(function () {
  const { Button, IconButton, Icon, Input, Badge, Banner, Checkbox, Tooltip } = window.Bodega;
  const { PageHeader, Sheet } = window.Fin;
  const { Money, Issues, Why, CatPicker, RateEntry, PairView, ParkedSheet, TriageModal, mono } = window.Triage;
  const T = window.TriageData;

  const COLS = '26px 64px minmax(0,1fr) 138px 138px 186px 26px';
  const GROUPS = [
    { b: 0, label: 'Needs a category', hint: 'One decision each' },
    { b: 2, label: 'Proposed pairs', hint: 'Two rows that look like one transfer' },
    { b: 3, label: 'Priced roughly', hint: 'No rate within 14 days — Ledger used the nearest one' },
  ];

  function TriageScreen() {
    const [list, setList] = React.useState(T.QUEUE);
    const [sel, setSel] = React.useState([]);
    const [open, setOpen] = React.useState(null);
    const [vals, setVals] = React.useState({});
    const [done, setDone] = React.useState(0);
    const [parked, setParked] = React.useState(T.PARKED.count);
    const [parkedOpen, setParkedOpen] = React.useState(false);
    const [bulkOpen, setBulkOpen] = React.useState(false);
    const [bulkCat, setBulkCat] = React.useState(null);
    const [collapsed, setCollapsed] = React.useState([3]);
    const [toast, setToast] = React.useState(null);

    const counts = T.totals(list);
    const blocking = counts.cat + counts.pair;
    const say = (text) => { setToast(text); window.clearTimeout(say.t); say.t = window.setTimeout(() => setToast(null), 2600); };
    const patch = (id, p) => setVals((s) => ({ ...s, [id]: { ...(s[id] || {}), ...p } }));
    const drop = (ids) => { setList((l) => l.filter((x) => ids.indexOf(x.id) < 0)); setSel((s) => s.filter((id) => ids.indexOf(id) < 0)); };
    const toggle = (id) => setSel((s) => (s.indexOf(id) > -1 ? s.filter((x) => x !== id) : s.concat(id)));

    // The queue in one order, whatever is collapsed — the modal walks this.
    const flat = GROUPS.reduce((a, g) => a.concat(list.filter((x) => x.bucket === g.b)), []);
    const idx = flat.findIndex((x) => x.id === open);
    const cur = idx > -1 ? flat[idx] : null;
    const step = (d) => { const n = flat[idx + d]; if (n) setOpen(n.id); };
    // Resolving removes the row, so the entry that slid into this slot is next.
    const advance = (id) => { const i = flat.findIndex((x) => x.id === id); const n = flat[i + 1] || flat[i - 1]; setOpen(n ? n.id : null); };

    // One save. It resolves whatever the row was asking for; a rate you type just
    // replaces the approximation and the row leaves the rough group.
    const saveRow = (item) => {
      const v = vals[item.id] || {};
      const cat = v.cat || item.cat;
      const rate = Number(v.rate);
      if (item.needs.cat && !cat) return;
      advance(item.id); drop([item.id]); setDone((n) => n + 1);
      say(item.needs.cat
        ? 'Sorted — ' + T.CAT[cat].label + '.' + (rate > 0 ? ' Rate set to ' + T.rateStr(rate) + '.' : '')
        : 'Rate set to ' + T.rateStr(rate > 0 ? rate : item.rate) + '.');
    };

    const acceptGuess = (item) => {
      drop([item.id]); setDone((n) => n + 1);
      say('Sorted — ' + T.CAT[item.guess.cat].label + '.');
    };

    const park = (ids) => {
      if (ids.length === 1) advance(ids[0]); else setOpen(null);
      drop(ids); setParked((p) => p + ids.length);
      say(ids.length > 1 ? ids.length + ' rows parked.' : 'Parked. It keeps its money, and stops asking.');
    };

    const bulkTargets = () => list.filter((x) => sel.indexOf(x.id) > -1 && x.needs.cat).map((x) => x.id);
    const bulkApply = () => {
      const hit = bulkTargets();
      drop(hit); setDone((n) => n + hit.length); setBulkOpen(false); setBulkCat(null);
      say(`${hit.length} rows sorted into ${T.CAT[bulkCat].label}.`);
    };

    const resolvePair = (item, paired) => {
      advance(item.id); drop([item.id]); setDone((n) => n + 1);
      say(paired ? 'Paired.' : 'Left unpaired — the legs stay separate rows.');
    };

    const row = (item) => {
      const on = open === item.id;
      const checked = sel.indexOf(item.id) > -1;
      return (
        <div key={item.id} style={{ borderTop: '1px solid var(--border-subtle)', background: on ? 'var(--paper-050)' : checked ? 'var(--thyme-050)' : 'transparent' }}>
          <div style={{ display: 'grid', gridTemplateColumns: COLS, alignItems: 'center', gap: 'var(--space-3)', minHeight: 44, padding: '0 var(--space-7)' }}>
            <Checkbox checked={checked} onChange={() => toggle(item.id)} aria-label={'Select ' + item.raw} />
            <span style={{ ...mono, fontSize: 11.5, color: 'var(--text-tertiary)', whiteSpace: 'nowrap' }}>{item.short}</span>
            <button type="button" onClick={() => setOpen(item.id)}
              style={{ display: 'flex', flexDirection: 'column', gap: 1, minWidth: 0, padding: '6px 0', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left', fontFamily: 'var(--font-sans)' }}>
              <span style={{ fontSize: 'var(--body-sm-size)', color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {item.merchant || item.raw}
              </span>
              {item.merchant ? (
                <span style={{ ...mono, fontSize: 10.5, color: 'var(--text-placeholder)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{item.raw}</span>
              ) : null}
            </button>
            <span style={{ fontSize: 12, color: 'var(--text-tertiary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {item.account.name}<span style={{ color: 'var(--text-placeholder)' }}> · {item.account.detail}</span>
            </span>
            <span style={{ minWidth: 0, overflow: 'hidden' }}>
              {item.needs.cat && item.guess ? (
                <Tooltip label={item.guess.rule
                  ? 'Rule ' + item.guess.rule + ' · /' + T.RULES.find((r) => r.id === item.guess.rule).pattern + '/i'
                  : 'You sorted this here ' + item.guess.times + ' times'}>
                  <button type="button" onClick={() => acceptGuess(item)}
                    style={{
                      display: 'inline-flex', alignItems: 'center', gap: 5, maxWidth: '100%', padding: '2px 8px 3px', cursor: 'pointer',
                      borderRadius: 'var(--radius-sm)', background: 'transparent', border: '1px dashed var(--border-default)',
                      color: 'var(--text-secondary)', fontFamily: 'var(--font-sans)', fontSize: 11.5, whiteSpace: 'nowrap',
                    }}>
                    <Icon name="check" size={11} />{T.CAT[item.guess.cat].label}
                  </button>
                </Tooltip>
              ) : (
                <Issues item={item} hide={item.bucket === 0 ? ['cat'] : []} />
              )}
            </span>
            <span style={{ minWidth: 0, overflow: 'hidden' }}><Money row={item} size="sm" /></span>
            <IconButton icon="maximize-2" label="Open this row" size="sm" onClick={() => setOpen(item.id)} />
          </div>
        </div>
      );
    };

    const groupHead = (g) => {
      const items = list.filter((x) => x.bucket === g.b);
      const shut = collapsed.indexOf(g.b) > -1;
      const gsel = items.filter((i) => sel.indexOf(i.id) > -1).length;
      if (!items.length) return null;
      return (
        <React.Fragment key={g.b}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: '18px var(--space-7) 8px' }}>
            <button type="button" onClick={() => setCollapsed((c) => (shut ? c.filter((x) => x !== g.b) : c.concat(g.b)))}
              style={{ display: 'flex', alignItems: 'center', gap: 7, background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontFamily: 'var(--font-sans)' }}>
              <Icon name={shut ? 'chevron-right' : 'chevron-down'} size={15} color="var(--ink-400)" />
              <span style={{ fontSize: 'var(--title-3-size)', fontWeight: 600, letterSpacing: '-0.006em' }}>{g.label}</span>
              <span style={{ ...mono, fontSize: 13, color: 'var(--text-tertiary)' }}>{items.length}</span>
            </button>
            <span style={{ fontSize: 'var(--caption-size)', color: 'var(--text-tertiary)' }}>{g.hint}</span>
            <span style={{ flex: 1 }} />
            {!shut ? (
              <button type="button"
                onClick={() => setSel((s) => (gsel === items.length ? s.filter((id) => items.every((i) => i.id !== id)) : s.concat(items.filter((i) => s.indexOf(i.id) < 0).map((i) => i.id))))}
                style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '2px 0', color: 'var(--text-accent)', fontFamily: 'var(--font-sans)', fontSize: 'var(--body-sm-size)' }}>
                {gsel === items.length ? 'Clear these' : 'Select all ' + items.length}
              </button>
            ) : null}
          </div>
          {!shut ? items.map(row) : null}
        </React.Fragment>
      );
    };

    return (
      <div style={{ position: 'relative', flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', minHeight: 0, background: 'var(--surface-canvas)' }}>
        <PageHeader
          question="What still needs you?"
          answer={blocking ? `${blocking} rows need you` : 'Nothing needs you'}
          meta={<>
            <Badge tone="neutral" size="sm">{counts.cat} category</Badge>
            {counts.pair ? <Badge tone="info" size="sm">{counts.pair} pairs</Badge> : null}
            {counts.rough ? <Badge tone="warning" size="sm" dot>{counts.rough} approximate rates</Badge> : null}
            <span style={{ fontSize: 'var(--caption-size)', color: 'var(--text-tertiary)' }}>· {done} done in this sitting</span>
          </>}
          actions={<>
            {flat.length ? (
              <Tooltip label={counts.rough ? `${blocking} that need you, then ${counts.rough} with approximate rates` : `${blocking} in one run`}>
                <Button size="sm" variant="primary" iconStart="play" onClick={() => setOpen(flat[0].id)}>Sort all {flat.length}</Button>
              </Tooltip>
            ) : null}
            {parked ? <Button size="sm" iconStart="archive" onClick={() => setParkedOpen(true)}>Parked {parked}</Button> : null}
          </>}
        />

        <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', paddingBottom: 'var(--space-8)' }}>
          <div style={{ padding: '0 var(--space-7) var(--space-2)' }}>
            <Banner tone="warning" title="One transfer has a single leg">{T.INTEGRITY.text}</Banner>
          </div>

          {GROUPS.map(groupHead)}

          {!list.length ? (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 'var(--space-4)', padding: 'var(--space-8) var(--space-7)', textAlign: 'center' }}>
              <Icon name="check-check" size={28} color="var(--thyme-600)" />
              <h2 style={{ margin: 0, fontFamily: 'var(--font-display)', fontSize: 'var(--display-3-size)', fontWeight: 500 }}>Queue empty.</h2>
              <p style={{ margin: 0, maxWidth: 420, fontSize: 'var(--body-size)', lineHeight: 1.55, color: 'var(--text-secondary)' }}>
                {done} rows sorted in this sitting. The next Provincial statement will start the next one.
              </p>
            </div>
          ) : null}

          {parked ? (
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', margin: 'var(--space-6) var(--space-7) 0', padding: 'var(--space-4) var(--space-5)', background: 'var(--surface-raised)', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)' }}>
            <Icon name="archive" size={17} color="var(--ink-400)" />
            <span style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
              <span style={{ fontSize: 'var(--body-size)' }}>
                <span style={{ ...mono }}>{parked}</span> parked rows, out of the queue
              </span>
              <span style={{ fontSize: 'var(--caption-size)', color: 'var(--text-tertiary)', textWrap: 'pretty' }}>{T.PARKED.note}</span>
            </span>
            <Button size="sm" variant="ghost" onClick={() => setParkedOpen(true)}>Look at them</Button>
            </div>
          ) : null}
        </div>

        {sel.length ? (
          <div style={{
            position: 'absolute', left: '50%', bottom: 20, transform: 'translateX(-50%)', zIndex: 5,
            display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: '8px 10px 8px var(--space-4)',
            background: 'var(--surface-inverse)', color: 'var(--text-inverse)', borderRadius: 'var(--radius-pill)',
            boxShadow: 'var(--shadow-pop)', animation: 'bodega-rise var(--dur-fast) var(--ease-lift)',
          }}>
            <span style={{ fontSize: 'var(--body-sm-size)' }}><span style={{ ...mono }}>{sel.length}</span> selected</span>
            <span style={{ width: 1, height: 20, background: 'rgba(255,255,255,0.18)' }} />
            <button type="button" onClick={() => setBulkOpen(true)}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '5px 10px', borderRadius: 'var(--radius-pill)', background: 'rgba(255,255,255,0.12)', border: 'none', color: 'var(--text-inverse)', cursor: 'pointer', fontFamily: 'var(--font-sans)', fontSize: 'var(--body-sm-size)' }}>
              <Icon name="tag" size={14} />Set a category
            </button>
            <button type="button" onClick={() => park(sel)}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '5px 10px', borderRadius: 'var(--radius-pill)', background: 'transparent', border: 'none', color: 'var(--text-inverse)', cursor: 'pointer', fontFamily: 'var(--font-sans)', fontSize: 'var(--body-sm-size)' }}>
              <Icon name="archive" size={14} />Park
            </button>
            <button type="button" onClick={() => setSel([])}
              style={{ padding: '5px 8px', background: 'transparent', border: 'none', color: 'rgba(255,255,255,0.7)', cursor: 'pointer', fontFamily: 'var(--font-sans)', fontSize: 'var(--body-sm-size)' }}>
              Clear
            </button>
          </div>
        ) : null}

        <TriageModal item={cur} index={idx} total={flat.length}
          v={cur ? (vals[cur.id] || {}) : {}}
          patch={(p) => cur && patch(cur.id, p)}
          onSave={() => cur && saveRow(cur)}
          onPark={() => cur && park([cur.id])}
          onPair={() => cur && resolvePair(cur, true)}
          onReject={() => cur && resolvePair(cur, false)}
          onPrev={() => step(-1)} onNext={() => step(1)} onClose={() => setOpen(null)} />

        <Sheet open={bulkOpen} onClose={() => setBulkOpen(false)} size="md"
          title={`Sort ${bulkTargets().length} rows at once`}
          description="Rows in the selection that already have a category are left alone."
          footer={<>
            <Button variant="ghost" onClick={() => setBulkOpen(false)}>Cancel</Button>
            <Button variant="primary" iconStart="check" disabled={!bulkCat} onClick={bulkApply}>Sort {bulkTargets().length} rows</Button>
          </>}>
          <CatPicker value={bulkCat} onChange={setBulkCat} columns={4} keys={false} />
        </Sheet>

        <ParkedSheet open={parkedOpen} count={parked} onClose={() => setParkedOpen(false)}
          onCutoff={(when) => { setParkedOpen(false); say(`Parking everything uncategorised before ${T.date(when)}.`); }}
          onBringBack={() => {
            setParkedOpen(false);
            setList((l) => T.order(T.PARKED.sample.filter((p) => l.every((x) => x.id !== p.id)).concat(l)));
            say(`${parked} rows back in the queue — oldest first.`);
            setParked(0);
          }} />

        {toast ? (
          <div style={{
            position: 'absolute', left: '50%', bottom: sel.length ? 74 : 22, transform: 'translateX(-50%)', zIndex: 'var(--z-toast)',
            display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderRadius: 'var(--radius-md)',
            background: 'var(--surface-inverse)', color: 'var(--text-inverse)', boxShadow: 'var(--shadow-pop)',
            fontSize: 'var(--body-sm-size)', animation: 'bodega-rise var(--dur-base) var(--ease-lift)',
          }}>
            <Icon name="check" size={14} />{toast}
          </div>
        ) : null}
      </div>
    );
  }

  window.Triage.TriageScreen = TriageScreen;
})();
