// Shared triage parts: the money pair, the provenance chip, the 26-category
// picker, the rate field, the pair confirm, and Park. Both candidate surfaces
// are assembled out of these, so the choice between them is structural.
window.Triage = window.Triage || {};

(function () {
  const { Icon, Button, IconButton, Input, Badge, Tooltip, Banner } = window.Bodega;
  const T = window.TriageData;

  const mono = { fontFamily: 'var(--font-mono)', fontVariantNumeric: 'tabular-nums' };

  // ── The consolidated USD number, with the native amount and its rate under it.
  function Money({ row, size = 'md', align = 'right' }) {
    const sizes = { sm: 13, md: 15.5, lg: 22, xl: 30 };
    const priced = row.usdValue != null;
    const positive = row.amount > 0;
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 3, alignItems: align === 'right' ? 'flex-end' : 'flex-start', minWidth: 0 }}>
        {priced ? (
          <span style={{ ...mono, fontSize: sizes[size], fontWeight: size === 'lg' || size === 'xl' ? 500 : 400, letterSpacing: size === 'lg' || size === 'xl' ? '-0.03em' : '-0.01em', color: positive ? 'var(--thyme-600)' : 'var(--text-primary)', whiteSpace: 'nowrap' }}>
            {T.usd(row.usdValue, { signed: true })}
          </span>
        ) : size === 'sm' ? (
          <span style={{ fontSize: 12.5, fontWeight: 500, color: 'var(--ochre-700)', whiteSpace: 'nowrap' }}>Unpriced</span>
        ) : (
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: size === 'lg' || size === 'xl' ? 17 : 13, fontWeight: 500, color: 'var(--ochre-700)', whiteSpace: 'nowrap' }}>
            <Icon name="circle-slash" size={size === 'lg' || size === 'xl' ? 17 : 13} />Can’t be priced
          </span>
        )}
        <span style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: size === 'sm' ? 'nowrap' : 'wrap', justifyContent: align === 'right' ? 'flex-end' : 'flex-start' }}>
          <span style={{ ...mono, fontSize: size === 'sm' ? 11 : 12, color: 'var(--text-tertiary)', whiteSpace: 'nowrap' }}>
            {T.native(row.amount, row.currency)}
          </span>
          {row.currency === 'VES' && row.rateSource !== 'none' ? <Prov row={row} showRate={size !== 'sm'} /> : null}
        </span>
      </div>
    );
  }

  // ── Where the number came from. Five tiers, and two of them need a warning.
  function Prov({ row, full = false, showRate = true }) {
    const src = T.RATE[row.rateSource];
    if (!src) return null;
    const warn = src.tone === 'ochre' || !!row.rough;
    const good = src.tone === 'thyme' && !row.rough;
    return (
      <Tooltip label={row.rough ? row.rough + ' — approximate' : src.note}>
        <span style={{
          display: 'inline-flex', alignItems: 'center', gap: 4, padding: '1px 6px 2px', borderRadius: 'var(--radius-sm)',
          background: warn ? 'var(--ochre-050)' : good ? 'var(--thyme-050)' : 'var(--paper-100)',
          border: `1px solid ${warn ? 'var(--ochre-200)' : good ? 'var(--thyme-100)' : 'var(--border-subtle)'}`,
          color: warn ? 'var(--ochre-700)' : good ? 'var(--thyme-700)' : 'var(--text-tertiary)',
          fontSize: 10.5, whiteSpace: 'nowrap', cursor: 'help',
        }}>
          {warn && !row.rough ? <Icon name="triangle-alert" size={10} /> : null}
          {row.rough ? '≈' : ''}{full ? src.label : src.short}
          {showRate && row.rate ? <span style={{ ...mono, opacity: 0.75 }}>{T.rateStr(row.rate)}</span> : null}
        </span>
      </Tooltip>
    );
  }

  // ── What is wrong with this row. One row can carry two badges.
  function Issues({ item, size = 'sm', hide = [] }) {
    const show = (k) => hide.indexOf(k) < 0;
    return (
      <span style={{ display: 'inline-flex', gap: 5, flexWrap: 'wrap' }}>
        {item.needs.cat && show('cat') ? <Badge tone="neutral" size={size}>Category</Badge> : null}
        {item.needs.rate && show('rate') ? <Badge tone="warning" size={size} dot>Rate</Badge> : null}
        {item.needs.pair && show('pair') ? <Badge tone="info" size={size}>Pair</Badge> : null}
      </span>
    );
  }

  // ── The merchant string as the bank wrote it, plus the cleaned name.
  function Merchant({ item, size = 'md' }) {
    const big = size === 'lg';
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: big ? 3 : 1, minWidth: 0 }}>
        <span style={{ fontSize: big ? 'var(--title-2-size)' : 'var(--body-sm-size)', fontWeight: big ? 600 : 500, letterSpacing: big ? '-0.01em' : 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {item.merchant || item.raw}
        </span>
        <span style={{ ...mono, fontSize: big ? 12 : 11, color: 'var(--text-placeholder)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {item.merchant ? item.raw : item.account.name + ' · ' + item.account.detail}
        </span>
      </div>
    );
  }

  // ── Why we think we know. A rule cites itself; history counts itself.
  function Why({ guess, onTake }) {
    if (!guess) return null;
    const c = T.CAT[guess.cat];
    const rule = guess.rule ? T.RULES.find((r) => r.id === guess.rule) : null;
    return (
      <button type="button" onClick={onTake}
        style={{
          display: 'flex', alignItems: 'center', gap: 8, padding: '8px 10px', textAlign: 'left', cursor: 'pointer', width: '100%',
          borderRadius: 'var(--radius-md)', background: 'var(--paper-200)', border: '1px dashed var(--border-default)', color: 'var(--text-secondary)',
        }}>
        <Icon name={rule ? 'file-code' : 'history'} size={14} color="var(--ink-500)" />
        <span style={{ flex: 1, minWidth: 0, fontSize: 'var(--body-sm-size)', lineHeight: 1.45 }}>
          <strong style={{ color: 'var(--text-primary)' }}>{c.label}</strong>
          {rule
            ? <> — rule {rule.id} matches <span style={{ ...mono, fontSize: 11.5 }}>/{rule.pattern}/i</span>{rule.min ? ' over $' + rule.min.toLocaleString('en-US') : ''}</>
            : <> — you sorted this here {guess.times} times</>}
        </span>
        <span style={{ ...mono, fontSize: 10.5, letterSpacing: '0.08em', textTransform: 'uppercase', color: 'var(--text-signal)' }}>use it</span>
      </button>
    );
  }

  // ── 26 categories: eight usage-ranked chips, search over the rest, and the
  // test that settles the edge visible at the moment of choosing.
  function CatPicker({ value, onChange, columns = 2, keys = true }) {
    const [q, setQ] = React.useState('');
    const [all, setAll] = React.useState(false);
    const [hover, setHover] = React.useState(null);
    const query = q.trim().toLowerCase();
    const matches = query
      ? T.PICKABLE.filter((c) => c.label.toLowerCase().indexOf(query) > -1 || c.test.toLowerCase().indexOf(query) > -1)
      : null;
    const shown = hover || value;
    const def = shown ? T.CAT[shown] : null;

    const chip = (c, i) => {
      const on = value === c.key;
      return (
        <button key={c.key} type="button" onClick={() => onChange(c.key)}
          onMouseEnter={() => setHover(c.key)} onMouseLeave={() => setHover(null)}
          style={{
            display: 'flex', alignItems: 'center', gap: 8, padding: '8px 9px', textAlign: 'left', minHeight: 38, cursor: 'pointer',
            borderRadius: 'var(--radius-md)', background: on ? 'var(--surface-selected)' : 'var(--surface-raised)',
            border: `1px solid ${on ? 'var(--thyme-300)' : 'var(--border-default)'}`, boxShadow: on ? 'none' : 'var(--shadow-xs)',
            color: on ? 'var(--thyme-800)' : 'var(--text-primary)', fontSize: 'var(--body-sm-size)', fontWeight: on ? 600 : 400,
            transition: 'var(--transition-control)',
          }}>
          <Icon name={c.icon} size={15} color={on ? 'var(--thyme-600)' : 'var(--text-tertiary)'} />
          <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{c.label}</span>
          {keys && i != null ? <span style={{ ...mono, fontSize: 10.5, color: on ? 'var(--thyme-600)' : 'var(--text-placeholder)' }}>{i + 1}</span> : null}
        </button>
      );
    };

    const row = (c) => {
      const on = value === c.key;
      return (
        <button key={c.key} type="button" onClick={() => onChange(c.key)}
          style={{
            display: 'flex', alignItems: 'flex-start', gap: 9, padding: '7px 9px', textAlign: 'left', width: '100%', cursor: 'pointer',
            borderRadius: 'var(--radius-md)', background: on ? 'var(--surface-selected)' : 'transparent',
            border: `1px solid ${on ? 'var(--thyme-300)' : 'transparent'}`, color: 'var(--text-primary)',
          }}>
          <Icon name={c.icon} size={15} color={on ? 'var(--thyme-600)' : 'var(--text-tertiary)'} style={{ marginTop: 2 }} />
          <span style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <span style={{ fontSize: 'var(--body-sm-size)', fontWeight: on ? 600 : 500 }}>{c.label}</span>
            <span style={{ fontSize: 'var(--caption-size)', lineHeight: 1.4, color: 'var(--text-tertiary)', textWrap: 'pretty' }}>{c.test}</span>
          </span>
          <span style={{ fontSize: 10.5, color: 'var(--text-placeholder)', textTransform: 'uppercase', letterSpacing: '0.06em', marginTop: 3 }}>{T.KIND_LABEL[c.kind]}</span>
        </button>
      );
    };

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)', minWidth: 0 }}>
        <Input size="sm" iconStart="search" value={q} onChange={(e) => { setQ(e.target.value); if (e.target.value) setAll(false); }}
          placeholder={`Search ${T.PICKABLE.length} categories`} aria-label="Search categories" />

        {matches ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2, maxHeight: 268, overflowY: 'auto', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', padding: 4, background: 'var(--paper-050)' }}>
            {matches.length ? matches.map(row) : (
              <span style={{ padding: '10px 8px', fontSize: 'var(--body-sm-size)', color: 'var(--text-tertiary)' }}>Nothing matches “{q}”. All {T.PICKABLE.length} are here — try the test rather than the name.</span>
            )}
          </div>
        ) : all ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2, maxHeight: 268, overflowY: 'auto', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', padding: 4, background: 'var(--paper-050)' }}>
            {['expense', 'income'].map((k) => (
              <React.Fragment key={k}>
                <span className="bodega-eyebrow" style={{ padding: '6px 8px 2px' }}>{T.KIND_LABEL[k]}</span>
                {T.PICKABLE.filter((c) => c.kind === k).map(row)}
              </React.Fragment>
            ))}
          </div>
        ) : (
          <>
            <div style={{ display: 'grid', gridTemplateColumns: `repeat(${columns}, minmax(0,1fr))`, gap: 6 }}>
              {T.TOP8.map((k, i) => chip(T.CAT[k], i))}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
              <button type="button" onClick={() => setAll(true)}
                style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: '4px 0', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-accent)', fontSize: 'var(--body-sm-size)', fontFamily: 'var(--font-sans)' }}>
                <Icon name="chevron-down" size={14} />The other {T.PICKABLE.length - 8}
              </button>
              <span style={{ fontSize: 11, color: 'var(--text-placeholder)' }}>Top eight by your last 12 months</span>
            </div>
          </>
        )}

        <div style={{ display: 'flex', gap: 7, padding: '7px 9px', minHeight: 44, borderRadius: 'var(--radius-md)', background: def ? 'var(--paper-100)' : 'transparent', border: `1px solid ${def ? 'var(--border-subtle)' : 'transparent'}` }}>
          {def ? (
            <>
              <Icon name="scale" size={13} color="var(--ink-400)" style={{ marginTop: 2, flex: '0 0 auto' }} />
              <span style={{ fontSize: 'var(--caption-size)', lineHeight: 1.45, color: 'var(--text-secondary)', textWrap: 'pretty' }}>
                <strong style={{ color: 'var(--text-primary)' }}>{def.label}</strong> — {def.test}
              </span>
            </>
          ) : null}
        </div>
      </div>
    );
  }

  // ── Overriding an approximate rate. The row already has a dollar figure; this is
  // where you replace it with the rate you actually got.
  function RateEntry({ item, value, onChange }) {
    const hints = T.rateHints(item);
    const rate = Number(value);
    const preview = rate > 0 ? item.amount / rate : null;
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
        <div style={{ display: 'flex', gap: 'var(--space-3)', padding: 'var(--space-3) var(--space-4)', background: 'var(--status-warning-bg)', border: '1px solid var(--status-warning-border)', borderRadius: 'var(--radius-md)' }}>
          <Icon name="triangle-alert" size={16} color="var(--status-warning-text)" style={{ marginTop: 1, flex: '0 0 auto' }} />
          <span style={{ fontSize: 'var(--body-sm-size)', lineHeight: 1.5, color: 'var(--text-secondary)', textWrap: 'pretty' }}>
            Priced at <span style={{ ...mono }}>{T.rateStr(item.rate)}</span> — {item.rough}. No P2P sell or BCV scrape within 14 days of {item.date}, so the dollar figure is an approximation.
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 'var(--space-4)' }}>
          <Input label="Rate you got" hint="Bolívares per dollar" size="md" inputMode="decimal" value={value}
            onChange={(e) => onChange(e.target.value)} placeholder={T.rateStr(item.rate)} style={{ ...mono }} containerStyle={{ flex: '0 0 176px' }} />
          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4, paddingBottom: 2 }}>
            <span className="bodega-eyebrow">{preview != null ? 'Would become' : 'Currently'}</span>
            <span style={{ ...mono, fontSize: 20, fontWeight: 500, letterSpacing: '-0.03em' }}>
              {T.usd(preview != null ? preview : item.usdValue, { signed: true })}
            </span>
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span className="bodega-eyebrow">Or take one of these</span>
          {hints.map((h) => (
            <button key={h.label} type="button" onClick={() => onChange(String(h.rate))}
              style={{
                display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: '8px 10px', cursor: 'pointer', textAlign: 'left',
                borderRadius: 'var(--radius-md)', background: 'var(--surface-raised)', border: '1px solid var(--border-default)', boxShadow: 'var(--shadow-xs)',
              }}>
              <span style={{ ...mono, fontSize: 14, minWidth: 62 }}>{T.rateStr(h.rate)}</span>
              <span style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 1 }}>
                <span style={{ fontSize: 'var(--body-sm-size)' }}>{h.label}</span>
                <span style={{ fontSize: 11, color: 'var(--text-tertiary)' }}>{h.note}</span>
              </span>
              <span style={{ ...mono, fontSize: 12, color: 'var(--text-tertiary)' }}>{T.usd(item.amount / h.rate)}</span>
            </button>
          ))}
        </div>
      </div>
    );
  }

  // ── A proposed pairing: two legs, one transfer_id, or a refusal with a reason.
  function PairView({ item, onConfirm, onReject, compact = false }) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
        <div style={{ border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', overflow: 'hidden', background: 'var(--surface-raised)' }}>
          {item.legs.map((leg, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: '10px 12px', borderTop: i ? '1px solid var(--border-subtle)' : 'none' }}>
              <Icon name={leg.account.icon} size={16} color="var(--ink-400)" />
              <span style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 1 }}>
                <span style={{ fontSize: 'var(--body-sm-size)', fontWeight: 500 }}>{leg.account.name}</span>
                <span style={{ ...mono, fontSize: 11, color: 'var(--text-placeholder)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{leg.raw} · {T.shortDate(leg.when)}</span>
              </span>
              <span style={{ ...mono, fontSize: 14, color: leg.amount > 0 ? 'var(--thyme-600)' : 'var(--text-primary)', whiteSpace: 'nowrap' }}>
                {T.native(leg.amount, leg.currency, { signed: true })}
              </span>
            </div>
          ))}
        </div>

        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 'var(--space-3)', fontSize: 'var(--caption-size)', color: 'var(--text-tertiary)' }}>
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
            <Icon name="git-compare-arrows" size={13} />{Math.round(item.confidence * 100)}% confident
          </span>
          <span>{item.days === 0 ? 'Same day' : item.days + ' day' + (item.days > 1 ? 's' : '') + ' apart'}</span>
          <span>{item.drift ? item.drift + '% rate drift' : 'Exact amounts'}</span>
          {item.implied ? <span style={{ ...mono }}>implies {T.rateStr(item.implied)} Bs./$</span> : null}
        </div>

        {item.refuse ? (
          <Banner tone="danger" title="This one cannot be confirmed">{item.refuse}</Banner>
        ) : item.note ? (
          <span style={{ fontSize: 'var(--body-sm-size)', lineHeight: 1.5, color: 'var(--text-secondary)', textWrap: 'pretty' }}>{item.note}</span>
        ) : null}

        {onConfirm ? (
          <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
            <Button variant="primary" size={compact ? 'sm' : 'md'} iconStart="link" disabled={!!item.refuse} onClick={onConfirm}>Pair them</Button>
            <Button variant="ghost" size={compact ? 'sm' : 'md'} onClick={onReject}>Not a pair</Button>
          </div>
        ) : null}
      </div>
    );
  }

  // ── Parked: the durable "not now". One cutoff date, and a way back in.
  function ParkedSheet({ open, onClose, count, onCutoff, onBringBack }) {
    const { Sheet } = window.Fin;
    const [when, setWhen] = React.useState(T.PARKED.before);
    const n = count != null ? count : T.PARKED.count;
    return (
      <Sheet open={open} onClose={onClose} size="md" title={`${n} parked rows`}
        description="Out of the queue, still in every balance and every report."
        footer={<>
          <Button variant="ghost" iconStart="undo-2" onClick={onBringBack}>Bring back all {n}</Button>
          <Button variant="primary" onClick={() => onCutoff(when)}>Done</Button>
        </>}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
          <Input type="date" label="Park uncategorised rows before" hint={`The oldest one is ${T.date(T.PARKED.oldest)}`} value={when} onChange={(e) => setWhen(e.target.value)} />

          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <span className="bodega-eyebrow">A few of them</span>
            <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-md)', overflow: 'hidden' }}>
              {T.PARKED.sample.map((p, i) => (
                <div key={p.id} style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: '8px 10px', borderTop: i ? '1px solid var(--border-subtle)' : 'none' }}>
                  <span style={{ ...mono, fontSize: 11, color: 'var(--text-tertiary)', flex: '0 0 62px' }}>{p.short}</span>
                  <span style={{ ...mono, flex: 1, minWidth: 0, fontSize: 11, color: 'var(--text-placeholder)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{p.raw}</span>
                  <span style={{ ...mono, fontSize: 11.5, color: 'var(--text-tertiary)' }}>{T.native(p.amount, p.currency)}</span>
                </div>
              ))}
            </div>
          </div>

          <span style={{ fontSize: 'var(--caption-size)', lineHeight: 1.5, color: 'var(--text-tertiary)', textWrap: 'pretty' }}>
            Their money still counts everywhere. Re-importing a statement will not push them back into the queue, and every badge they were carrying is still on them when you come back.
          </span>
        </div>
      </Sheet>
    );
  }

  // ── One entry, full attention. The queue stays behind it; ←/→ walks the same
  // modal across entries, so a sitting is one opening and one closing.
  function TriageModal({ item, index, total, v, patch, onSave, onPark, onPair, onReject, onPrev, onNext, onClose }) {
    const ready = item ? (item.needs.cat ? !!(v.cat || item.cat) : Number(v.rate) > 0) : false;

    React.useEffect(() => {
      if (!item) return;
      const k = (e) => {
        if (e.target && /input|textarea/i.test(e.target.tagName) && e.key !== 'Escape') return;
        if (e.key === 'Escape') return onClose();
        if (e.key === 'ArrowRight') { e.preventDefault(); return onNext(); }
        if (e.key === 'ArrowLeft') { e.preventDefault(); return onPrev(); }
        if (e.key === 'Enter' && ready) { e.preventDefault(); return onSave(); }
        const n = Number(e.key);
        if (item.needs.cat && n >= 1 && n <= T.TOP8.length) patch({ cat: T.TOP8[n - 1] });
      };
      document.addEventListener('keydown', k);
      return () => document.removeEventListener('keydown', k);
    }, [item, ready, onClose, onNext, onPrev, onSave, patch]);

    if (!item) return null;
    const both = item.needs.cat && item.rough;

    const facts = (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', minWidth: 0 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
          <Money row={item} size="lg" align="left" />
          <Merchant item={item} size="lg" />
          <span style={{ fontSize: 'var(--body-sm-size)', color: 'var(--text-tertiary)' }}>
            {item.date} · {item.account.name}<span style={{ color: 'var(--text-placeholder)' }}> · {item.account.detail}</span>
          </span>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}><Issues item={item} /></div>
      </div>
    );

    const decision = item.needs.pair ? (
      <PairView item={item} onConfirm={onPair} onReject={onReject} />
    ) : (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-5)', minWidth: 0 }}>
        {item.needs.cat ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-3)', minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 'var(--space-3)' }}>
              <span className="bodega-eyebrow">What was this for?</span>
              <span style={{ ...mono, fontSize: 11, color: 'var(--text-placeholder)' }}>1–8</span>
            </div>
            <Why guess={item.guess} onTake={() => patch({ cat: item.guess.cat })} />
            <CatPicker value={v.cat || item.cat} onChange={(c) => patch({ cat: c })} columns={2} />
          </div>
        ) : null}
        {item.rough ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-3)', minWidth: 0 }}>
            <span className="bodega-eyebrow">{both ? 'And the rate, if you know it' : 'The rate is a guess — replace it?'}</span>
            <RateEntry item={item} value={v.rate || ''} onChange={(r) => patch({ rate: r })} />
          </div>
        ) : null}
        <Input size="sm" placeholder="Note — optional" value={v.note || ''} onChange={(e) => patch({ note: e.target.value })} />
      </div>
    );

    return (
      <div onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
        style={{
          position: 'absolute', inset: 0, zIndex: 'var(--z-overlay)', display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 'var(--space-6)', background: 'var(--scrim)', backdropFilter: 'var(--blur-overlay)', WebkitBackdropFilter: 'var(--blur-overlay)',
          animation: 'bodega-fade var(--dur-fast) var(--ease-standard)',
        }}>
        <div role="dialog" aria-modal="true" aria-label="Resolve this row"
          style={{
            width: '100%', maxWidth: 880, height: 'min(680px, calc(100% - 24px))', display: 'flex', flexDirection: 'column',
            background: 'var(--surface-raised)', borderRadius: 'var(--radius-xl)', boxShadow: 'var(--shadow-pop)', overflow: 'hidden',
            animation: 'bodega-rise var(--dur-base) var(--ease-lift)',
          }}>
          <header style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: 'var(--space-4) var(--space-4) var(--space-3) var(--space-5)', borderBottom: '1px solid var(--border-subtle)' }}>
            <span className="bodega-eyebrow" style={{ whiteSpace: 'nowrap' }}>{index + 1} of {total}</span>
            <span style={{ flex: '0 1 140px', minWidth: 64, height: 4, borderRadius: 2, background: 'var(--paper-200)', overflow: 'hidden' }}>
              <span style={{ display: 'block', width: ((index + 1) / total * 100) + '%', height: '100%', borderRadius: 2, background: 'var(--thyme-600)', transition: 'width var(--dur-fast) var(--ease-standard)' }} />
            </span>
            <span style={{ flex: 1 }} />
            <IconButton icon="chevron-left" label="Previous row" size="sm" disabled={index === 0} onClick={onPrev} />
            <IconButton icon="chevron-right" label="Next row" size="sm" disabled={index >= total - 1} onClick={onNext} />
            <IconButton icon="x" label="Close" size="sm" onClick={onClose} />
          </header>

          <div style={{
            flex: 1, minHeight: 0, overflow: 'hidden', display: 'grid', gap: 0,
            gridTemplateColumns: 'minmax(0,0.78fr) minmax(0,1.22fr)',
          }}>
            <div style={{ minHeight: 0, overflowX: 'hidden', overflowY: 'auto', padding: 'var(--space-5)', borderRight: '1px solid var(--border-subtle)' }}>{facts}</div>
            <div style={{ minHeight: 0, overflowX: 'hidden', overflowY: 'auto', padding: 'var(--space-5)' }}>{decision}</div>
          </div>

          <footer style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', padding: 'var(--space-4)', borderTop: '1px solid var(--border-subtle)', background: 'var(--paper-050)' }}>
            <Button variant="ghost" iconStart="archive" onClick={onPark}>Park</Button>
            <span style={{ fontSize: 11, color: 'var(--text-tertiary)' }}>
              <span style={{ ...mono }}>←→</span> move · <span style={{ ...mono }}>↵</span> save · <span style={{ ...mono }}>esc</span> close
            </span>
            <span style={{ flex: 1 }} />
            {!item.needs.pair ? (
              <Button variant="primary" iconStart="check" disabled={!ready} onClick={onSave}>
                {index + 1 < total ? (item.needs.cat ? 'Sort and next' : 'Use this rate and next') : 'Save and finish'}
              </Button>
            ) : null}
          </footer>
        </div>
      </div>
    );
  }

  // A queue row's date, dense. Bank rows have no time component, so the day is
  // all there is — no "Today / Yesterday" theatre.
  function When({ item }) {
    return <span style={{ ...mono, fontSize: 11.5, color: 'var(--text-tertiary)', whiteSpace: 'nowrap' }}>{item.short}</span>;
  }

  Object.assign(window.Triage, { Money, Prov, Issues, Merchant, Why, CatPicker, RateEntry, PairView, ParkedSheet, TriageModal, When, mono });
})();
