// Shell furniture for Ledger: the rail, the page header, the money type,
// and the desktop/mobile viewport switch.
window.Fin = window.Fin || {};

(function () {
  const { SideNav, Icon, IconButton, Tooltip } = window.Bodega;
  const D = window.FinData;

  // One place decides how much air a screen gets, so mobile stays honest.
  const gutter = (compact) => (compact ? '0 var(--space-4) 104px' : '0 var(--space-7) var(--space-8)');

  // Every figure in the app goes through this so alignment and colour are consistent.
  function Amount({ value, size = 'md', signed = false, tone, style }) {
    const sizes = { sm: 12.5, md: 'var(--num-size)', lg: 20, xl: 30, hero: 52 };
    const figure = size === 'hero';
    const colour = tone === 'positive' ? 'var(--thyme-600)'
      : tone === 'negative' ? 'var(--clay-600)'
      : tone === 'muted' ? 'var(--text-tertiary)'
      : 'var(--text-primary)';
    return (
      <span style={{
        fontFamily: figure ? 'var(--font-display)' : 'var(--font-mono)', fontVariantNumeric: 'tabular-nums',
        fontSize: sizes[size] || sizes.md, letterSpacing: figure ? '-0.02em' : size === 'xl' ? '-0.03em' : '-0.01em',
        fontWeight: figure ? 700 : size === 'xl' ? 500 : 400, lineHeight: 1.05, color: colour, whiteSpace: 'nowrap', ...style,
      }}>
        {signed && value > 0 ? '+' : ''}{D.money(value)}
      </span>
    );
  }

  function Avatar() {
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '0 4px', minWidth: 0 }}>
        <span style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 24, height: 24, borderRadius: 'var(--radius-xs)', background: 'var(--red-500)', color: 'var(--paper-050)', fontSize: 10.5, fontWeight: 600, flex: '0 0 auto' }}>RK</span>
        <span style={{ flex: '1 1 0', minWidth: 0, fontSize: 12.5, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>Rae Kimura</span>
        <Tooltip label="Settings" style={{ flex: '0 0 auto' }}><IconButton icon="settings" label="Settings" size="sm" /></Tooltip>
      </div>
    );
  }

  function Wordmark({ size = 19 }) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 9, padding: '0 6px' }}>
        <span style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: size * 1.37, height: size * 1.37, borderRadius: 'var(--radius-sm)', background: 'var(--red-500)', color: 'var(--paper-050)', fontFamily: 'var(--font-display)', fontSize: size * 0.84, fontWeight: 600, flex: '0 0 auto' }}>L</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: size * 0.86, fontWeight: 700, letterSpacing: '0.01em' }}>
          Ledger<span style={{ color: 'var(--red-500)' }}>.</span>
        </span>
      </div>
    );
  }

  // Five destinations, each phrased as the question it answers.
  const DESTINATIONS = [
    { value: 'today',    label: 'Today',    icon: 'sun' },
    { value: 'flow',     label: 'Flow',     icon: 'arrow-left-right' },
    { value: 'plans',    label: 'Plans',    icon: 'target' },
    { value: 'ahead',    label: 'Ahead',    icon: 'route' },
    { value: 'accounts', label: 'Accounts', icon: 'landmark' },
  ];

  function Rail({ route, onRoute, reviewCount }) {
    return (
      <SideNav
        value={route} onChange={onRoute} header={<Wordmark />} footer={<Avatar />}
        sections={[{ items: DESTINATIONS.map((d) => (d.value === 'flow' ? { ...d, count: reviewCount || undefined } : d)) }]}
      />
    );
  }

  // Mobile: the same five destinations, thumb-height, with the one primary action floating.
  function MobileTabBar({ route, onRoute, reviewCount, onAdd }) {
    return (
      <>
        <button type="button" onClick={onAdd} aria-label="Add transaction"
          style={{
            position: 'absolute', right: 16, bottom: 80, width: 52, height: 52, borderRadius: 'var(--radius-md)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer',
            background: 'var(--red-500)', color: 'var(--paper-050)', border: '1px solid var(--border-accent)',
            boxShadow: 'var(--shadow-pop)', zIndex: 3,
          }}>
          <Icon name="plus" size={22} strokeWidth={2} />
        </button>
        <nav style={{
          position: 'absolute', left: 0, right: 0, bottom: 0, zIndex: 4,
          display: 'grid', gridTemplateColumns: `repeat(${DESTINATIONS.length}, 1fr)`,
          background: 'var(--surface-raised)', borderTop: '1px solid var(--border-default)',
          padding: '6px 4px 22px', boxShadow: 'var(--shadow-lg)',
        }}>
          {DESTINATIONS.map((d) => {
            const on = route === d.value;
            const badge = d.value === 'flow' ? reviewCount : 0;
            return (
              <button key={d.value} type="button" onClick={() => onRoute(d.value)}
                style={{
                  position: 'relative', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3,
                  padding: '8px 0 6px', minHeight: 44, background: 'transparent', border: 'none', cursor: 'pointer',
                  color: on ? 'var(--thyme-700)' : 'var(--text-tertiary)',
                }}>
                <Icon name={d.icon} size={19} strokeWidth={on ? 2 : 1.6} />
                <span style={{ fontSize: 10.5, fontWeight: on ? 600 : 400, letterSpacing: '-0.004em' }}>{d.label}</span>
                {badge ? (
                  <span style={{ position: 'absolute', top: 4, right: '50%', marginRight: -18, minWidth: 15, height: 15, padding: '0 4px', borderRadius: 'var(--radius-pill)', background: 'var(--ochre-500)', color: 'var(--paper-050)', fontSize: 9.5, fontWeight: 600, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{badge}</span>
                ) : null}
              </button>
            );
          })}
        </nav>
      </>
    );
  }

  // The phone the mobile view lives inside. Bezel only — no drawn hardware.
  function PhoneFrame({ children }) {
    return (
      <div style={{ width: '100%', height: '100%', overflowY: 'auto', display: 'flex', alignItems: 'flex-start', justifyContent: 'center', padding: '28px 0 40px', background: 'var(--paper-200)' }}>
        <div style={{
          position: 'relative', width: 390, height: 844, flex: '0 0 auto', borderRadius: 44, padding: 11,
          background: 'var(--ink-900, #1c1a17)', boxShadow: 'var(--shadow-pop)',
        }}>
          <div style={{ position: 'relative', width: '100%', height: '100%', borderRadius: 34, overflow: 'hidden', background: 'var(--surface-canvas)', display: 'flex', flexDirection: 'column' }}>
            <div style={{ flex: '0 0 auto', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '11px 22px 5px', fontSize: 12.5, fontWeight: 600, color: 'var(--text-primary)', background: 'var(--surface-canvas)' }}>
              <span style={{ fontFamily: 'var(--font-mono)', letterSpacing: '-0.02em' }}>9:41</span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 5, color: 'var(--text-secondary)' }}>
                <Icon name="signal" size={14} /><Icon name="wifi" size={14} /><Icon name="battery-full" size={17} />
              </span>
            </div>
            <div style={{ position: 'relative', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>{children}</div>
          </div>
        </div>
      </div>
    );
  }

  // The laptop the desktop view lives inside: a fixed 1440×900 canvas, so the
  // layout is judged at a real screen size instead of stretching to the window.
  const LAPTOP = { w: 1440, h: 900 };
  function LaptopFrame({ children }) {
    const box = React.useRef(null);
    const [scale, setScale] = React.useState(1);
    React.useEffect(() => {
      const fit = () => {
        const el = box.current;
        if (!el) return;
        const pad = 40;
        setScale(Math.min(1, (el.clientWidth - pad) / LAPTOP.w, (el.clientHeight - pad) / LAPTOP.h));
      };
      fit();
      window.addEventListener('resize', fit);
      return () => window.removeEventListener('resize', fit);
    }, []);
    return (
      <div ref={box} style={{ width: '100%', height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--paper-200)', overflow: 'hidden' }}>
        <div style={{
          position: 'relative', width: LAPTOP.w, height: LAPTOP.h, flex: '0 0 auto',
          transform: `scale(${scale})`, transformOrigin: 'center center',
          borderRadius: 14, overflow: 'hidden', background: 'var(--surface-canvas)',
          border: '1px solid var(--border-default)', boxShadow: 'var(--shadow-pop)',
        }}>{children}</div>
      </div>
    );
  }

  // Prototype furniture, not product UI: flip the same screens between viewports.
  function ViewToggle({ view, onView }) {
    const opts = [{ v: 'desktop', icon: 'monitor', label: 'Desktop' }, { v: 'mobile', icon: 'smartphone', label: 'Mobile' }];
    return (
      <div style={{
        position: 'fixed', bottom: 18, right: 18, zIndex: 'var(--z-toast)',
        display: 'flex', gap: 2, padding: 3, borderRadius: 'var(--radius-pill)',
        background: 'var(--surface-raised)', border: '1px solid var(--border-default)', boxShadow: 'var(--shadow-pop)',
      }}>
        {opts.map((o) => {
          const on = view === o.v;
          return (
            <button key={o.v} type="button" onClick={() => onView(o.v)}
              style={{
                display: 'flex', alignItems: 'center', gap: 6, padding: '6px 12px', borderRadius: 'var(--radius-pill)',
                border: 'none', cursor: 'pointer', fontFamily: 'var(--font-sans)', fontSize: 'var(--body-sm-size)',
                fontWeight: on ? 600 : 400, background: on ? 'var(--thyme-700)' : 'transparent',
                color: on ? 'var(--paper-050)' : 'var(--text-secondary)', transition: 'var(--transition-control)',
              }}>
              <Icon name={o.icon} size={14} />{o.label}
            </button>
          );
        })}
      </div>
    );
  }

  // Every screen opens with the question it answers, then the answer.
  function PageHeader({ question, answer, meta, actions, compact }) {
    return (
      <header style={{
        display: 'flex', alignItems: compact ? 'stretch' : 'flex-end', justifyContent: 'space-between',
        flexDirection: compact ? 'column' : 'row', gap: compact ? 'var(--space-4)' : 'var(--space-6)',
        padding: compact ? 'var(--space-5) var(--space-4) var(--space-4)' : 'var(--space-7) var(--space-7) var(--space-5)',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, minWidth: 0 }}>
          <span className="bodega-kick">{question}</span>
          <h1 style={{ margin: 0, fontFamily: 'var(--font-display)', fontSize: compact ? 'var(--display-3-size)' : 'var(--display-2-size)', lineHeight: 1.05, letterSpacing: 'var(--display-2-tracking)', fontWeight: 700 }}>{answer}</h1>
          {meta ? <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', marginTop: 2 }}>{meta}</div> : null}
        </div>
        {actions ? <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', flex: '0 0 auto', flexWrap: 'wrap' }}>{actions}</div> : null}
      </header>
    );
  }

  function CatIcon({ cat, size = 32, tone }) {
    const c = D.CAT[cat];
    return (
      <span style={{
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        width: size, height: size, borderRadius: 'var(--radius-md)', flex: '0 0 auto',
        background: tone === 'unclear' ? 'var(--ochre-050)' : 'var(--paper-100)',
        border: `1px solid ${tone === 'unclear' ? 'var(--ochre-200)' : 'var(--border-subtle)'}`,
        color: tone === 'unclear' ? 'var(--ochre-600)' : 'var(--ink-500)',
      }}>
        <Icon name={c ? c.icon : 'circle-help'} size={Math.round(size * 0.5)} strokeWidth={1.6} />
      </span>
    );
  }

  // A single horizontal bar that explains a total by its parts.
  function SplitBar({ parts, height = 10 }) {
    const total = parts.reduce((s, p) => s + p.value, 0);
    return (
      <div style={{ display: 'flex', width: '100%', height, borderRadius: 'var(--radius-pill)', overflow: 'hidden', background: 'var(--paper-200)' }}>
        {parts.map((p, i) => (
          <span key={i} title={p.label} style={{ width: `${(p.value / total) * 100}%`, background: p.color, borderRight: i < parts.length - 1 ? '1.5px solid var(--surface-raised)' : 'none' }} />
        ))}
      </div>
    );
  }

  function Legend({ parts }) {
    return (
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 'var(--space-4)' }}>
        {parts.map((p, i) => (
          <div key={i} style={{ display: 'flex', flexDirection: 'column', gap: 3, minWidth: 0 }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: p.color, flex: '0 0 auto' }} />
              <span style={{ fontSize: 'var(--caption-size)', color: 'var(--text-secondary)' }}>{p.label}</span>
            </span>
            <Amount value={p.value} size="md" style={{ paddingLeft: 14 }} />
          </div>
        ))}
      </div>
    );
  }

  // Modal shell. Positioned absolute, not fixed, so it stays inside whichever
  // viewport is being previewed — the phone frame clips it like a real device would.
  // Height is fixed per size: the frame never resizes under content, the body scrolls.
  const SHEET_WIDTHS = { sm: 420, md: 560, lg: 760 };
  const SHEET_HEIGHTS = { sm: 420, md: 520, lg: 640 };
  function Sheet({ open, title, description, size = 'md', compact, onClose, footer, children }) {
    React.useEffect(() => {
      if (!open || !onClose) return;
      const k = (e) => { if (e.key === 'Escape') onClose(); };
      document.addEventListener('keydown', k);
      return () => document.removeEventListener('keydown', k);
    }, [open, onClose]);
    if (!open) return null;
    return (
      <div onClick={(e) => { if (e.target === e.currentTarget && onClose) onClose(); }}
        style={{
          position: 'absolute', inset: 0, zIndex: 'var(--z-overlay)', display: 'flex',
          alignItems: compact ? 'flex-end' : 'center', justifyContent: 'center',
          padding: compact ? 0 : 'var(--space-6)',
          background: 'var(--scrim)', backdropFilter: 'var(--blur-overlay)', WebkitBackdropFilter: 'var(--blur-overlay)',
          animation: 'bodega-fade var(--dur-fast) var(--ease-standard)',
        }}>
        <div role="dialog" aria-modal="true" aria-label={typeof title === 'string' ? title : 'Dialog'}
          style={{
            width: '100%', maxWidth: compact ? 'none' : SHEET_WIDTHS[size] || SHEET_WIDTHS.md,
            height: compact ? undefined : `min(${SHEET_HEIGHTS[size] || SHEET_HEIGHTS.md}px, calc(100% - 48px))`,
            maxHeight: compact ? '94%' : 'calc(100% - 48px)', display: 'flex', flexDirection: 'column',
            background: 'var(--surface-raised)', borderRadius: compact ? '20px 20px 0 0' : 'var(--radius-xl)',
            boxShadow: 'var(--shadow-pop)', overflow: 'hidden', animation: 'bodega-rise var(--dur-base) var(--ease-lift)',
          }}>
          <header style={{ display: 'flex', alignItems: 'flex-start', gap: 'var(--space-4)', padding: 'var(--space-5) var(--space-5) var(--space-3)' }}>
            <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 4 }}>
              {title ? <h2 style={{ margin: 0, fontSize: 'var(--title-2-size)', lineHeight: 'var(--title-2-line)', letterSpacing: 'var(--title-2-tracking)', fontWeight: 'var(--weight-semibold)' }}>{title}</h2> : null}
              {description ? <p style={{ margin: 0, fontSize: 'var(--body-size)', lineHeight: 'var(--body-line)', color: 'var(--text-tertiary)' }}>{description}</p> : null}
            </div>
            {onClose ? <IconButton icon="x" label="Close" size="sm" onClick={onClose} /> : null}
          </header>
          <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: '0 var(--space-5) var(--space-5)' }}>{children}</div>
          {footer ? (
            <footer style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 'var(--space-2)', padding: 'var(--space-3) var(--space-5)', borderTop: '1px solid var(--border-subtle)', background: 'var(--paper-050)' }}>{footer}</footer>
          ) : null}
        </div>
      </div>
    );
  }

  // A panel laid over a chart — the quick read, with a way through to everything.
  function ChartOverlay({ open, onClose, children, maxWidth = 460 }) {
    if (!open) return null;
    return (
      <div onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
        style={{
          position: 'absolute', inset: 0, zIndex: 2, display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 'var(--space-3)', background: 'color-mix(in oklab, var(--surface-raised) 72%, transparent)',
          backdropFilter: 'var(--blur-overlay)', WebkitBackdropFilter: 'var(--blur-overlay)',
          animation: 'bodega-fade var(--dur-fast) var(--ease-standard)',
        }}>
        <div style={{
          width: '100%', maxWidth, display: 'flex', flexDirection: 'column', gap: 'var(--space-3)',
          padding: 'var(--space-4)', borderRadius: 'var(--radius-lg)', background: 'var(--surface-raised)',
          border: '1px solid var(--border-default)', boxShadow: 'var(--shadow-pop)',
          animation: 'bodega-rise var(--dur-fast) var(--ease-lift)',
        }}>{children}</div>
      </div>
    );
  }

  Object.assign(window.Fin, { Rail, MobileTabBar, PhoneFrame, LaptopFrame, ChartOverlay, ViewToggle, PageHeader, Amount, CatIcon, SplitBar, Legend, Wordmark, Sheet, gutter, DESTINATIONS });
})();
