// Triage fixtures, shaped like the real repo instead of like the old fixture app.
// Every amount carries a currency and a rate provenance; the queue carries three
// item types; rows can be parked. Names and figures are plausible, not real.
window.TriageData = (function () {
  const NBSP = '\u00a0';
  const MINUS = '\u2212';

  const group = (n, dp) => Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp });
  // Sign before symbol, always: −$1,200.00, never $−1,200.00 (00-design.md).
  const usd = (n, opts) => (n < 0 ? MINUS : (opts && opts.signed && n > 0 ? '+' : '')) + '$' + group(n, 2);
  const ves = (n, opts) => (n < 0 ? MINUS : (opts && opts.signed && n > 0 ? '+' : '')) + 'Bs.' + NBSP + group(n, 2);
  const usdt = (n, opts) => (n < 0 ? MINUS : (opts && opts.signed && n > 0 ? '+' : '')) + group(n, 2) + NBSP + 'USDT';
  const native = (n, currency, opts) => (currency === 'VES' ? ves(n, opts) : currency === 'USDT' ? usdt(n, opts) : usd(n, opts));
  const rateStr = (r) => group(r, 2);

  const WD = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const MO = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const THIS_YEAR = 2026;
  const parse = (s) => new Date(s.slice(0, 10) + 'T12:00:00');
  // Mon, Jul 7 — year appended only when it is not the current year.
  const date = (s) => {
    const d = parse(s);
    return WD[d.getDay()] + ', ' + MO[d.getMonth()] + ' ' + d.getDate() + (d.getFullYear() === THIS_YEAR ? '' : ', ' + d.getFullYear());
  };
  const shortDate = (s) => { const d = parse(s); return MO[d.getMonth()] + ' ' + d.getDate() + (d.getFullYear() === THIS_YEAR ? '' : ' ' + String(d.getFullYear()).slice(2)); };

  // ── Accounts: six kinds, each with its own currency. Balances are derived. ──
  const ACCOUNTS = {
    prov:  { id: 'prov',  name: 'Provincial',      detail: '0108 · 4471', kind: 'bank',           currency: 'VES',  source: 'Provincial CSV', icon: 'landmark' },
    spot:  { id: 'spot',  name: 'Binance Spot',    detail: 'USDT',        kind: 'crypto_spot',    currency: 'USDT', source: 'Binance API',    icon: 'bitcoin' },
    fund:  { id: 'fund',  name: 'Binance Funding', detail: 'P2P wallet',  kind: 'crypto_funding', currency: 'USDT', source: 'Binance API',    icon: 'wallet' },
    earn:  { id: 'earn',  name: 'Binance Earn',    detail: 'Flexible',    kind: 'crypto_earn',    currency: 'USDT', source: 'Binance API',    icon: 'percent' },
    cashd: { id: 'cashd', name: 'Cash',            detail: 'US dollars',  kind: 'cash',           currency: 'USD',  source: 'By hand',        icon: 'banknote' },
    cashb: { id: 'cashb', name: 'Cash',            detail: 'Bolívares',   kind: 'cash',           currency: 'VES',  source: 'By hand',        icon: 'banknote' },
  };

  // ── The five rate tiers, in resolver order. ─────────────────────────────────
  const RATE = {
    user:     { key: 'user',     label: 'Your rate',      short: 'yours',    note: 'You typed this rate for this row', tone: 'thyme' },
    realized: { key: 'realized', label: 'Your cost basis', short: 'realized', note: 'The rate you actually got on a P2P sell within 14 days', tone: 'thyme' },
    median:   { key: 'median',   label: 'P2P median',     short: 'median',   note: '14-day median of Binance P2P sells', tone: 'quiet' },
    bcv:      { key: 'bcv',      label: 'BCV',            short: 'BCV',      note: 'Official floor — no P2P rate within 14 days of this row', tone: 'ochre', fallback: true },
    none:     { key: 'none',     label: "Can't be priced", short: 'none',    note: 'No rate available for this date. Type one and the row prices itself.', tone: 'ochre' },
  };

  const RATES_TODAY = { realized: 165.4, median: 162.75, bcv: 148.2, medianAge: '12 minutes ago', bcvAge: 'today, 8:05' };

  // ── 26 categories in four kinds. Each carries the test that settles its edge;
  // those rulings live in category-definitions.md and in no UI until now. ──────
  const CATS = [
    { key: 'groceries',   label: 'Groceries',     kind: 'expense', icon: 'shopping-basket', use: 214, test: 'Food you cook at home. Prepared food you ate out is Going Out.' },
    { key: 'going-out',   label: 'Going Out',     kind: 'expense', icon: 'utensils',        use: 168, test: 'Any meal or drink out — including the dinner you paid for a friend.' },
    { key: 'transport',   label: 'Transport',     kind: 'expense', icon: 'car',             use: 96,  test: 'Fuel, fares, parking, repairs.' },
    { key: 'utilities',   label: 'Utilities',     kind: 'expense', icon: 'zap',             use: 74,  test: 'Power, water, internet, phone top-ups.' },
    { key: 'personal',    label: 'Personal Care', kind: 'expense', icon: 'scissors',        use: 58,  test: 'Pharmacy, barber, cosmetics — unless it is a prescription, which is Health.' },
    { key: 'purchases',   label: 'Purchases',     kind: 'expense', icon: 'package',         use: 44,  test: 'Things that last: electronics, furniture, tools.' },
    { key: 'leisure',     label: 'Leisure',       kind: 'expense', icon: 'ticket',          use: 37,  test: 'Tickets, hobbies, trips. A meal on a trip is still Going Out.' },
    { key: 'subs',        label: 'Subscriptions', kind: 'expense', icon: 'repeat',          use: 31,  test: 'Anything that charges on a cycle.' },
    { key: 'health',      label: 'Health',        kind: 'expense', icon: 'heart-pulse',     use: 26,  test: 'Doctors, tests, prescriptions. Pharmacy without one is Personal Care.' },
    { key: 'family',      label: 'Family',        kind: 'expense', icon: 'users',           use: 24,  test: "Money to family you don't expect back. If you expect it back it is Lending." },
    { key: 'lending',     label: 'Lending',       kind: 'expense', icon: 'hand-coins',      use: 19,  test: 'Money you expect back. Always — even from family.' },
    { key: 'dating',      label: 'Dating',        kind: 'expense', icon: 'heart',           use: 17,  test: 'Spending on a date, whoever it was for.' },
    { key: 'clothing',    label: 'Clothing',      kind: 'expense', icon: 'shirt',           use: 15,  test: 'Clothes, shoes, bags.', off: true },
    { key: 'rent',        label: 'Rent',          kind: 'expense', icon: 'house',           use: 14,  test: 'Rent and condo fees.' },
    { key: 'gifts',       label: 'Gifts',         kind: 'expense', icon: 'gift',            use: 12,  test: 'For someone else, nothing expected back.' },
    { key: 'education',   label: 'Education',     kind: 'expense', icon: 'graduation-cap',  use: 6,   test: 'Courses, books, certifications.' },
    { key: 'other-exp',   label: 'Other Expense', kind: 'expense', icon: 'circle-dashed',   use: 9,   test: 'Only when nothing else is honest. Review these later.' },
    { key: 'fees',        label: 'Fees',          kind: 'expense', icon: 'receipt',         use: 88,  test: 'Bank and exchange fees.', auto: true },
    { key: 'salary',      label: 'Salary',        kind: 'income',  icon: 'briefcase',       use: 22,  test: 'Your regular pay, whichever account it lands in.' },
    { key: 'gigs',        label: 'Gigs',          kind: 'income',  icon: 'laptop',          use: 18,  test: 'Freelance and side work.' },
    { key: 'loan-repay',  label: 'Loan Repayment', kind: 'income', icon: 'rotate-ccw',      use: 11,  test: 'Money coming back that you lent out.' },
    { key: 'other-inc',   label: 'Other Income',  kind: 'income',  icon: 'arrow-down-left', use: 5,   test: 'Income with no better home.' },
    { key: 'interest',    label: 'Interest',      kind: 'income',  icon: 'percent',         use: 61,  test: 'Binance Earn interest.', auto: true },
    { key: 'transfer',    label: 'Transfer',      kind: 'transfer', icon: 'arrow-left-right', use: 132, test: 'Both legs of a move between your own accounts.', auto: true },
    { key: 'opening',     label: 'Opening position', kind: 'adjustment', icon: 'flag',      use: 6,   test: 'The starting balance of an account.', auto: true },
    { key: 'reconcile',   label: 'Reconciliation', kind: 'adjustment', icon: 'scale',       use: 4,   test: 'Explains a difference you corrected by hand.', auto: true },
  ];
  const CAT = CATS.reduce((m, c) => { m[c.key] = c; return m; }, {});
  // Auto-only categories and the ones you told me you never pick both stay out of
  // the picker. Nothing is deleted — retired categories are deactivated, never dropped.
  const PICKABLE = CATS.filter((c) => !c.auto && !c.off);
  const TOP8 = PICKABLE.slice().sort((a, b) => b.use - a.use).slice(0, 8).map((c) => c.key);
  const KIND_LABEL = { expense: 'Expense', income: 'Income', transfer: 'Transfer', adjustment: 'Adjustment' };

  // ── Rules: scoped regex, priority, amount bounds. Managed in migrations. ────
  const RULES = [
    { id: 12, pattern: 'cuota cashea|cashea', cat: 'purchases', priority: 40, scope: 'any source' },
    { id: 7,  pattern: '^binance deposit', cat: 'salary', priority: 10, scope: 'Binance Spot', min: 1000, note: 'A Binance deposit over $1,000' },
    { id: 21, pattern: 'excelsior gama|gran gama|plazas', cat: 'groceries', priority: 50, scope: 'Provincial' },
    { id: 24, pattern: 'farmatodo', cat: 'personal', priority: 50, scope: 'Provincial' },
    { id: 26, pattern: 'corpoelec|cantv|digitel', cat: 'utilities', priority: 50, scope: 'Provincial' },
  ];

  // ── The queue. `needs` is what is actually wrong with the row; bucket is the
  // difficulty order the service sorts by (0 category, 1 rate, 2 pair). ───────
  // r: rate source key. VES rows with r 'none' have no amount_usd at all.
  const rows = [
    // bucket 0 — a category is the only thing missing.
    ['q0101', '2026-07-03', 'EXCELSIOR GAMA PLUS 4471', 'Excelsior Gama', 'prov', -3412.0, 'median', ['cat'], { cat: 'groceries', why: 'rule', rule: 21 }],
    ['q0102', '2026-07-03', 'FARMATODO EL CAFETAL', 'Farmatodo', 'prov', -1845.5, 'median', ['cat'], { cat: 'personal', why: 'rule', rule: 24 }],
    ['q0103', '2026-07-03', 'PAGO MOVIL 04141234567', null, 'prov', -900.0, 'median', ['cat'], null],
    ['q0104', '2026-07-03', 'DIGITEL RECARGA', 'Digitel', 'prov', -240.0, 'median', ['cat'], { cat: 'utilities', why: 'rule', rule: 26 }],
    ['q0105', '2026-07-04', 'BINANCE PAY MERCHANT', 'Binance Pay', 'spot', -18.4, null, ['cat'], null],
    ['q0106', '2026-07-05', 'CUOTA CASHEA 2/4 TRAKI', 'Cashea · Traki', 'prov', -2480.0, 'median', ['cat'], { cat: 'purchases', why: 'rule', rule: 12 }],
    ['q0107', '2026-07-05', 'ALMUERZO', 'Lunch, cash', 'cashd', -6.0, null, ['cat'], null],
    ['q0108', '2026-07-05', 'COMPRA POS 5075 GRAN GAMA', 'Gran Gama', 'prov', -5120.75, 'median', ['cat'], { cat: 'groceries', why: 'rule', rule: 21 }],
    ['q0109', '2026-07-06', 'TRANSF A/FAVOR MAMA 000771', null, 'prov', -4000.0, 'median', ['cat'], { cat: 'family', why: 'history', times: 6 }],
    ['q0110', '2026-07-06', 'MERCADOLIBRE VE', 'Mercado Libre', 'prov', -7340.0, 'median', ['cat'], { cat: 'purchases', why: 'history', times: 3 }],
    ['q0111', '2026-07-07', 'UBER TRIP HELP.UBER.COM', 'Uber', 'prov', -1120.0, 'median', ['cat'], { cat: 'transport', why: 'history', times: 11 }],
    ['q0112', '2026-07-07', 'CINEX SAMBIL', 'Cinex', 'prov', -960.0, 'median', ['cat'], null],
    ['q0113', '2026-07-07', 'PEDIDOSYA', 'PedidosYa', 'prov', -2240.0, 'median', ['cat'], { cat: 'going-out', why: 'history', times: 8 }],
    ['q0114', '2026-07-07', 'CLINICA LA FLORESTA', 'Clínica La Floresta', 'prov', -12400.0, 'median', ['cat'], null, { bigger: 'Four times bigger than anything else you have spent there.' }],
    ['q0115', '2026-07-08', 'BARBERIA DON JOSE', 'Barber', 'cashd', -8.0, null, ['cat'], { cat: 'personal', why: 'history', times: 4 }],
    ['q0116', '2026-07-08', 'CONDOMINIO RES LOS PINOS', 'Condo fee', 'prov', -6800.0, 'median', ['cat'], { cat: 'rent', why: 'history', times: 9 }],
    ['q0117', '2026-07-08', 'PAGO MOVIL 04249876543', null, 'prov', -1500.0, 'median', ['cat'], null],
    ['q0118', '2026-07-09', 'AMAZON MKTPLACE', 'Amazon', 'spot', -42.1, null, ['cat'], null],
    ['q0119', '2026-07-09', 'GASOLINA PDV', 'Fuel', 'cashb', -180.0, 'median', ['cat'], { cat: 'transport', why: 'history', times: 7 }],
    ['q0120', '2026-07-10', 'SUPERMERCADO PLAZAS', 'Plazas', 'prov', -3980.0, 'median', ['cat'], { cat: 'groceries', why: 'rule', rule: 21 }],
    ['q0121', '2026-07-10', 'IPOSTEL', 'Ipostel', 'prov', -120.0, 'median', ['cat'], null],
    ['q0122', '2026-07-11', 'ZOOM ENVIOS', 'Zoom', 'prov', -380.0, 'median', ['cat'], null],
    ['q0123', '2026-07-11', 'CANTV ABA', 'Cantv', 'prov', -450.0, 'median', ['cat'], { cat: 'utilities', why: 'rule', rule: 26 }],
    ['q0124', '2026-07-12', 'PAGO MOVIL 04121110099', null, 'prov', -2650.0, 'median', ['cat'], null],
    // bucket 1 — the resolver could not price these. Four also need a category.
    ['q0201', '2026-02-12', 'COMPRA POS 8841 TRAKI', 'Traki', 'prov', -8940.0, 'none', ['rate', 'cat'], null],
    ['q0202', '2026-02-14', 'PAGO MOVIL 04121112233', null, 'prov', -2200.0, 'none', ['rate', 'cat'], null],
    ['q0203', '2026-03-02', 'TRANSF 000988 A/FAVOR', 'Client payment', 'prov', 16500.0, 'none', ['rate'], null, { cat: 'gigs' }],
    ['q0204', '2026-03-02', 'COMPRA POS 1123 FARMATODO', 'Farmatodo', 'prov', -990.0, 'none', ['rate'], { cat: 'personal', why: 'rule', rule: 24 }, { cat: 'personal' }],
    ['q0205', '2026-03-03', 'CORPOELEC', 'Corpoelec', 'prov', -310.0, 'none', ['rate'], null, { cat: 'utilities' }],
    ['q0206', '2026-03-05', 'EFECTIVO BS MERCADO', null, 'cashb', -1750.0, 'none', ['rate', 'cat'], null],
    ['q0207', '2026-03-09', 'COMPRA POS 4410 GRAN GAMA', 'Gran Gama', 'prov', -4220.0, 'none', ['rate'], null, { cat: 'groceries' }],
    ['q0208', '2026-03-11', 'PAGO MOVIL 04160001122', null, 'prov', -5400.0, 'none', ['rate', 'cat'], null],
    ['q0209', '2026-03-14', 'CUOTA CASHEA 3/4 FARMATODO', 'Cashea · Farmatodo', 'prov', -1240.0, 'none', ['rate'], null, { cat: 'purchases' }],
    ['q0210', '2026-03-16', 'DIGITEL RECARGA', 'Digitel', 'prov', -260.0, 'none', ['rate'], null, { cat: 'utilities' }],
  ];

  // No P2P sell and no BCV scrape within 14 days: Ledger prices the row with the
  // nearest rate it has and marks the figure as approximate, rather than leaving it
  // with no dollar value at all.
  const NEAREST = {
    '2026-02': [{ key: 'bcv', rate: 141.8, label: 'BCV, 3 days later', note: 'Nearest scraped official rate' }, { key: 'median', rate: 152.4, label: 'P2P median, 21 days later', note: 'Outside the 14-day window' }],
    '2026-03': [{ key: 'bcv', rate: 144.6, label: 'BCV, same week', note: 'Nearest scraped official rate' }, { key: 'median', rate: 158.2, label: 'P2P median, 19 days later', note: 'Outside the 14-day window' }],
  };
  const nearest = (when) => (NEAREST[when.slice(0, 7)] || [{ key: 'bcv', rate: RATES_TODAY.bcv, label: 'BCV today', note: 'Official floor' }])[0];

  const buildRow = (r) => {
    const [id, when, raw, clean, acct, amount, rateKey, needs, guess, extra] = r;
    const a = ACCOUNTS[acct];
    const rough = rateKey === 'none' ? nearest(when) : null;
    const rate = rough ? rough.rate : rateKey ? RATES_TODAY[rateKey] : 1;
    const item = {
      id, occurred: when, date: date(when), short: shortDate(when),
      raw, merchant: clean, account: a, currency: a.currency,
      amount, rate, rateSource: rough ? rough.key : rateKey || null,
      rough: rough ? rough.label + ' · ' + rough.note : null,
      usdValue: rate ? amount / rate : null,
      kind: amount > 0 ? 'income' : 'expense',
      needs: { cat: needs.indexOf('cat') > -1, rate: false, pair: false },
      guess, cat: (extra && extra.cat) || null, bigger: extra && extra.bigger,
      source: a.source, sourceRef: a.kind === 'bank' ? 'provincial_2026_07.csv · line ' + (100 + Number(id.slice(-3))) : null,
    };
    if (item.currency !== 'VES') { item.rateSource = null; item.rough = null; } // 1:1, nothing to explain
    // A category is what blocks a row. An approximate rate is worth a look, not a stop.
    item.bucket = item.needs.cat ? 0 : 3;
    return item;
  };

  const QUEUE = rows.map(buildRow);

  // ── Pair proposals: two rows that should share one transfer_id. ─────────────
  const pairs = [
    {
      id: 'q0301', occurred: '2026-07-07', confidence: 0.96, days: 0, drift: 0.4,
      legs: [
        { raw: 'TRANSF A/FAVOR 000123', account: ACCOUNTS.prov, amount: 45231.1, currency: 'VES', when: '2026-07-07' },
        { raw: 'P2P SELL USDT/VES', account: ACCOUNTS.fund, amount: -277.9, currency: 'USDT', when: '2026-07-07' },
      ],
      implied: 162.76, note: 'Confirming this makes it your cost basis for every VES row in the next 14 days.',
    },
    {
      id: 'q0302', occurred: '2026-07-06', confidence: 1, days: 0, drift: 0,
      legs: [
        { raw: 'RETIRO EFECTIVO 04471', account: ACCOUNTS.prov, amount: -8000.0, currency: 'VES', when: '2026-07-06' },
        { raw: 'EFECTIVO BS', account: ACCOUNTS.cashb, amount: 8000.0, currency: 'VES', when: '2026-07-06' },
      ],
      implied: null, note: 'Same currency, same amount, same day.',
    },
    {
      id: 'q0303', occurred: '2026-07-02', confidence: 0.81, days: 1, drift: 1.6,
      legs: [
        { raw: 'TRANSF A/FAVOR 000119', account: ACCOUNTS.prov, amount: 12300.0, currency: 'VES', when: '2026-07-02' },
        { raw: 'P2P SELL USDT/VES', account: ACCOUNTS.fund, amount: -74.5, currency: 'USDT', when: '2026-07-01' },
      ],
      implied: 165.1, note: 'A day apart, and 1.6% off the rate you got. Plausible, not certain.',
    },
    {
      id: 'q0304', occurred: '2026-06-24', confidence: 0.44, days: 7, drift: 12.4,
      refuse: 'Seven days apart and 12.4% of drift. Confirming is refused above five days or ten percent.',
      legs: [
        { raw: 'TRANSF A/FAVOR 000104', account: ACCOUNTS.prov, amount: 30000.0, currency: 'VES', when: '2026-06-24' },
        { raw: 'P2P SELL USDT/VES', account: ACCOUNTS.fund, amount: -171.2, currency: 'USDT', when: '2026-06-17' },
      ],
      implied: 175.2, note: null,
    },
  ].map((p) => ({
    ...p, bucket: 2, date: date(p.occurred), short: shortDate(p.occurred),
    needs: { cat: false, rate: false, pair: true }, type: 'pair',
    account: p.legs[0].account, amount: p.legs[0].amount, currency: p.legs[0].currency,
    raw: p.legs[0].raw, merchant: null,
    usdValue: p.legs[0].currency === 'VES' ? p.legs[0].amount / RATES_TODAY.median : p.legs[0].amount,
    rateSource: null, source: 'Binance API + Provincial CSV',
  }));

  // Difficulty, then age, then id — 204 of 243 live rows share a timestamp, so the
  // id tiebreak is what keeps the order stable at all.
  const order = (items) => items.slice().sort((a, b) =>
    a.bucket - b.bucket || (a.occurred < b.occurred ? -1 : a.occurred > b.occurred ? 1 : 0) || (a.id < b.id ? -1 : 1));

  const ALL = order(QUEUE.concat(pairs));

  // Parked: out of the queue, still in every balance. One cutoff date does the work.
  const parkedRows = [
    ['p001', '2025-11-12', 'COMPRA POS 3311 TRAKI', 'Traki', 'prov', -6400.0, 'none', ['cat'], null],
    ['p002', '2025-09-03', 'PAGO MOVIL 04141234567', null, 'prov', -1200.0, 'none', ['cat'], null],
    ['p003', '2025-08-21', 'EFECTIVO BS', null, 'cashb', -3000.0, 'none', ['cat'], null],
  ].map(buildRow);

  const PARKED = {
    count: 266, before: '2026-01-01', oldest: '2024-11-03',
    note: 'Everything uncategorised before Jan 1, 2026. They keep their money in every balance and report, and they are still here when you want them.',
    sample: parkedRows,
  };

  const INTEGRITY = { count: 1, text: 'Binance Funding, Jun 29 — 96.40 USDT out with nothing on the other side. Pair it, or say it was not a transfer.' };

  const SOURCES = [
    { label: 'Provincial CSV', when: '6 days ago', stale: true, how: 'You drop the statement in' },
    { label: 'Binance API', when: '4 minutes ago', stale: false, how: 'Refreshed on open' },
    { label: 'P2P rates', when: RATES_TODAY.medianAge, stale: false, how: 'Scraper' },
    { label: 'BCV rate', when: RATES_TODAY.bcvAge, stale: false, how: 'Scraper' },
    { label: 'Cash', when: 'yesterday', stale: false, how: 'By hand' },
  ];

  // Suggestions offered when a row cannot be priced: nothing within 14 days, so
  // every option is an approximation and says so.
  const rateHints = (row) => NEAREST[row.occurred.slice(0, 7)] || [{ key: 'bcv', rate: RATES_TODAY.bcv, label: 'BCV today', note: 'Official floor' }];

  const totals = (items) => ({
    cat: items.filter((i) => i.needs.cat).length,
    pair: items.filter((i) => i.needs.pair).length,
    rough: items.filter((i) => i.rough && !i.needs.cat).length,
  });

  return {
    usd, ves, usdt, native, rateStr, date, shortDate, MINUS,
    ACCOUNTS, RATE, RATES_TODAY, CATS, CAT, PICKABLE, TOP8, KIND_LABEL, RULES,
    QUEUE: ALL, PAIRS: pairs, PARKED, INTEGRITY, SOURCES,
    order, rateHints, totals,
  };
})();
