// Fixture data for the Ledger personal-finances kit. Plausible, not real.
window.FinData = (function () {
  const money = (n, opts) => {
    const o = opts || {};
    const abs = Math.abs(n);
    const s = '$' + abs.toLocaleString('en-US', { minimumFractionDigits: o.cents === false ? 0 : 2, maximumFractionDigits: o.cents === false ? 0 : 2 });
    return n < 0 ? (o.paren ? '(' + s + ')' : '−' + s) : s;
  };

  // The three numbers the whole app is built around.
  const checking = 4182.60;
  const billsDue = 1640.00;
  const assigned = 1258.00;
  const safeToSpend = checking - billsDue - assigned;

  const accounts = [
    { id: 'a1', name: 'Everyday Checking', org: 'First Republic', kind: 'Cash', balance: 4182.60, last: '4412', icon: 'wallet' },
    { id: 'a2', name: 'Emergency fund', org: 'Ally Savings', kind: 'Cash', balance: 8400.00, last: '8891', icon: 'piggy-bank' },
    { id: 'a3', name: 'Japan trip', org: 'Ally Savings', kind: 'Cash', balance: 1905.00, last: '8892', icon: 'piggy-bank' },
    { id: 'a4', name: 'Visa Signature', org: 'Chase', kind: 'Credit', balance: -1247.80, last: '3021', icon: 'credit-card', detail: 'Statement due 2 Apr' },
    { id: 'a5', name: 'Brokerage', org: 'Vanguard', kind: 'Investments', balance: 32410.55, last: '0177', icon: 'chart-no-axes-column', detail: '+2.1% this month' },
    { id: 'a6', name: 'Roth IRA', org: 'Vanguard', kind: 'Investments', balance: 18220.00, last: '0178', icon: 'chart-no-axes-column', detail: '+1.8% this month' },
    { id: 'a7', name: 'Home', org: 'Estimated value', kind: 'Property', balance: 412000.00, icon: 'house' },
    { id: 'a8', name: 'Mortgage', org: 'Wells Fargo', kind: 'Property', balance: -214600.00, last: '5560', icon: 'landmark', detail: '22 years left' },
  ];

  const netWorth = accounts.reduce((s, a) => s + a.balance, 0);
  // Accounts a transaction can actually land in.
  const spendAccounts = accounts.filter((a) => a.kind === 'Cash' || a.kind === 'Credit').map((a) => a.name);

  const CAT = {
    groceries:  { label: 'Groceries',   icon: 'shopping-basket', plan: 'Groceries' },
    dining:     { label: 'Eating out',  icon: 'utensils',        plan: 'Eating out' },
    transport:  { label: 'Transport',   icon: 'train-front',     plan: 'Transport' },
    fun:        { label: 'Fun',         icon: 'ticket',          plan: 'Fun' },
    home:       { label: 'Home',        icon: 'house',           plan: 'Rent' },
    utilities:  { label: 'Utilities',   icon: 'zap',             plan: 'Electric' },
    health:     { label: 'Health',      icon: 'heart-pulse',     plan: 'Health' },
    subs:       { label: 'Subscriptions', icon: 'repeat',        plan: 'Subscriptions' },
    income:     { label: 'Income',      icon: 'arrow-down-left', plan: null },
    transfer:   { label: 'Transfer',    icon: 'arrow-left-right',plan: null },
  };
  // Ordered for pickers: the eight you sort into, then the two that sort themselves.
  const CAT_KEYS = ['groceries', 'dining', 'transport', 'fun', 'home', 'utilities', 'health', 'subs'];

  const tx = [
    { id: 't1',  date: '2026-03-14', day: 'Today',      merchant: 'Sunrise Market',      account: 'Everyday Checking', amount: -64.18,  cat: 'groceries', review: true,  note: '' },
    { id: 't2',  date: '2026-03-14', day: 'Today',      merchant: 'Kinto Coffee',        account: 'Visa Signature',    amount: -6.75,   cat: 'dining',    review: false },
    { id: 't3',  date: '2026-03-14', day: 'Today',      merchant: 'PORTLND PKG 4471',    account: 'Visa Signature',    amount: -28.00,  cat: null,        review: true,  unclear: true },
    { id: 't4',  date: '2026-03-13', day: 'Yesterday',  merchant: 'Trimet Hop Pass',     account: 'Everyday Checking', amount: -28.00,  cat: 'transport', review: false },
    { id: 't5',  date: '2026-03-13', day: 'Yesterday',  merchant: 'Powell\'s Books',     account: 'Visa Signature',    amount: -42.30,  cat: 'fun',       review: false },
    { id: 't6',  date: '2026-03-13', day: 'Yesterday',  merchant: 'Aurora Health',       account: 'Everyday Checking', amount: -180.00, cat: null,        review: true,  unclear: true, bigger: true },
    { id: 't7',  date: '2026-03-12', day: '12 March',   merchant: 'Netflix',             account: 'Visa Signature',    amount: -17.99,  cat: 'subs',      review: false, recurring: true },
    { id: 't8',  date: '2026-03-12', day: '12 March',   merchant: 'Sunrise Market',      account: 'Everyday Checking', amount: -112.44, cat: 'groceries', review: false },
    { id: 't9',  date: '2026-03-11', day: '11 March',   merchant: 'Ristretto Roasters',  account: 'Visa Signature',    amount: -14.50,  cat: 'dining',    review: false },
    { id: 't10', date: '2026-03-11', day: '11 March',   merchant: 'Transfer to Japan trip', account: 'Everyday Checking', amount: -300.00, cat: 'transfer', review: false },
    { id: 't11', date: '2026-03-10', day: '10 March',   merchant: 'Pacific Power',       account: 'Everyday Checking', amount: -96.12,  cat: 'utilities', review: false, recurring: true },
    { id: 't12', date: '2026-03-10', day: '10 March',   merchant: 'Bridgetown Studio',   account: 'Everyday Checking', amount: 2600.00, cat: 'income',    review: false, recurring: true },
    { id: 't13', date: '2026-03-09', day: '9 March',    merchant: 'Nong\'s Khao Man Gai', account: 'Visa Signature',   amount: -19.25,  cat: 'dining',    review: false },
    { id: 't14', date: '2026-03-08', day: '8 March',    merchant: 'Sunrise Market',      account: 'Everyday Checking', amount: -88.90,  cat: 'groceries', review: false },
    { id: 't15', date: '2026-03-07', merchant: 'Bike Farm',            account: 'Visa Signature',    amount: -54.00,  cat: 'transport', review: false },
    { id: 't16', date: '2026-03-06', merchant: 'Verizon',              account: 'Everyday Checking', amount: -45.00,  cat: 'utilities', review: false, recurring: true },
    { id: 't17', date: '2026-03-05', merchant: 'Hollywood Theatre',    account: 'Visa Signature',    amount: -38.00,  cat: 'fun',       review: false },
    { id: 't18', date: '2026-03-04', merchant: 'Ziply Fiber',          account: 'Everyday Checking', amount: -70.00,  cat: 'utilities', review: false, recurring: true },
    { id: 't19', date: '2026-03-03', merchant: 'Spotify',              account: 'Visa Signature',    amount: -11.99,  cat: 'subs',      review: false, recurring: true },
    { id: 't20', date: '2026-03-02', merchant: 'Cheryl\'s on 12th',    account: 'Visa Signature',    amount: -68.40,  cat: 'dining',    review: false },
    { id: 't21', date: '2026-03-01', merchant: 'Salmon Creek Rentals', account: 'Everyday Checking', amount: -1850.00, cat: 'home',     review: false, recurring: true },
    { id: 't22', date: '2026-02-27', merchant: 'Bridgetown Studio',    account: 'Everyday Checking', amount: 2600.00, cat: 'income',    review: false, recurring: true },
    { id: 't23', date: '2026-02-26', merchant: 'Sunrise Market',       account: 'Everyday Checking', amount: -104.10, cat: 'groceries', review: false },
    { id: 't24', date: '2026-02-24', merchant: 'Aurora Health',        account: 'Everyday Checking', amount: -45.00,  cat: 'health',    review: false },
    { id: 't25', date: '2026-02-22', merchant: 'Transfer to Emergency fund', account: 'Everyday Checking', amount: -400.00, cat: 'transfer', review: false },
    { id: 't26', date: '2026-02-19', merchant: 'Deschutes Brewery',    account: 'Visa Signature',    amount: -47.20,  cat: 'fun',       review: false },
    { id: 't27', date: '2026-02-17', merchant: 'Trimet Hop Pass',      account: 'Everyday Checking', amount: -28.00,  cat: 'transport', review: false },
    { id: 't28', date: '2026-02-14', merchant: 'Nostrana',            account: 'Visa Signature',    amount: -132.75, cat: 'dining',    review: false },
    { id: 't29', date: '2026-02-12', merchant: 'Netflix',              account: 'Visa Signature',    amount: -17.99,  cat: 'subs',      review: false, recurring: true },
    { id: 't30', date: '2026-02-10', merchant: 'Pacific Power',        account: 'Everyday Checking', amount: -118.40, cat: 'utilities', review: false, recurring: true },
    { id: 't31', date: '2026-02-10', merchant: 'Bridgetown Studio',    account: 'Everyday Checking', amount: 2600.00, cat: 'income',    review: false, recurring: true },
    { id: 't32', date: '2026-02-06', merchant: 'REI',                  account: 'Visa Signature',    amount: -212.00, cat: 'fun',       review: false },
    { id: 't33', date: '2026-02-02', merchant: 'Salmon Creek Rentals', account: 'Everyday Checking', amount: -1850.00, cat: 'home',     review: false, recurring: true },
    { id: 't34', date: '2026-01-28', merchant: 'Bridgetown Studio',    account: 'Everyday Checking', amount: 2600.00, cat: 'income',    review: false, recurring: true },
    { id: 't35', date: '2026-01-24', merchant: 'Sunrise Market',       account: 'Everyday Checking', amount: -97.65,  cat: 'groceries', review: false },
    { id: 't36', date: '2026-01-19', merchant: 'Broder Cafe',          account: 'Visa Signature',    amount: -41.00,  cat: 'dining',    review: false },
    { id: 't37', date: '2026-01-15', merchant: 'Providence Clinic',    account: 'Everyday Checking', amount: -165.00, cat: 'health',    review: false },
    { id: 't38', date: '2026-01-09', merchant: 'Powell\'s Books',      account: 'Visa Signature',    amount: -26.80,  cat: 'fun',       review: false },
    { id: 't39', date: '2026-01-05', merchant: 'Salmon Creek Rentals', account: 'Everyday Checking', amount: -1850.00, cat: 'home',     review: false, recurring: true },
  ];

  // ── Dates: everything the filters need, derived once from tx.date ────────────
  const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  const SHORT = MONTHS.map((m) => m.slice(0, 3));
  const DAY_MS = 86400000;
  const parse = (s) => new Date(s + 'T00:00:00');
  const today = parse('2026-03-14');
  const weekStart = (d) => { const x = new Date(d); x.setDate(x.getDate() - ((x.getDay() + 6) % 7)); x.setHours(0, 0, 0, 0); return x; };
  const thisWeek = weekStart(today);

  const enrich = (t) => {
    const d = parse(t.date);
    t.mkey = t.date.slice(0, 7);
    t.month = MONTHS[d.getMonth()] + ' ' + d.getFullYear();
    t.wkey = Math.round((thisWeek - weekStart(d)) / (7 * DAY_MS));
    if (!t.day) t.day = d.getDate() + ' ' + MONTHS[d.getMonth()];
    return t;
  };

  tx.forEach(enrich);

  const weekLabel = (n) => {
    const s = new Date(thisWeek); s.setDate(s.getDate() - n * 7);
    const e = new Date(s); e.setDate(e.getDate() + 6);
    const range = s.getMonth() === e.getMonth()
      ? `${s.getDate()}–${e.getDate()} ${SHORT[e.getMonth()]}`
      : `${s.getDate()} ${SHORT[s.getMonth()]} – ${e.getDate()} ${SHORT[e.getMonth()]}`;
    const name = n === 0 ? 'This week' : n === 1 ? 'Last week' : `${n} weeks ago`;
    return { name, range, label: `${name} · ${range}` };
  };

  const months = Array.from(new Set(tx.map((t) => t.mkey))).sort().reverse()
    .map((k) => ({ value: k, label: MONTHS[Number(k.slice(5, 7)) - 1] + ' ' + k.slice(0, 4) }));
  const weeks = Array.from(new Set(tx.map((t) => t.wkey))).sort((a, b) => a - b)
    .map((n) => ({ value: String(n), label: weekLabel(n).label }));

  const plans = [
    { group: 'Bills', hint: 'Due on a date, same-ish every month', items: [
      { id: 'p1', name: 'Rent',          assigned: 1850, spent: 1850, due: 'Paid 1 Mar',  icon: 'house' },
      { id: 'p2', name: 'Electric',      assigned: 110,  spent: 96.12, due: 'Paid 10 Mar', icon: 'zap' },
      { id: 'p3', name: 'Internet',      assigned: 70,   spent: 70,   due: 'Paid 4 Mar',  icon: 'wifi' },
      { id: 'p4', name: 'Phone',         assigned: 45,   spent: 45,   due: 'Paid 6 Mar',  icon: 'smartphone' },
      { id: 'p5', name: 'Car insurance', assigned: 128,  spent: 0,    due: 'Due 22 Mar',  icon: 'shield', upcoming: true },
      { id: 'p6', name: 'Subscriptions', assigned: 62,   spent: 47.98, due: 'Rolling',     icon: 'repeat' },
    ]},
    { group: 'Everyday', hint: 'Money you decide about week to week', items: [
      { id: 'p7',  name: 'Groceries',  assigned: 600, spent: 412.44, icon: 'shopping-basket' },
      { id: 'p8',  name: 'Eating out', assigned: 200, spent: 188.50, icon: 'utensils' },
      { id: 'p9',  name: 'Transport',  assigned: 120, spent: 64.00,  icon: 'train-front' },
      { id: 'p10', name: 'Fun',        assigned: 150, spent: 195.30, icon: 'ticket' },
    ]},
    { group: 'Saving for', hint: 'Money with a future job', items: [
      { id: 'p11', name: 'Japan trip',     assigned: 300, spent: 300, goal: 4800, saved: 1905, icon: 'plane' },
      { id: 'p12', name: 'Emergency fund', assigned: 400, spent: 400, goal: 12000, saved: 8400, icon: 'umbrella' },
    ]},
  ];

  const upcoming = [
    { id: 'u1', name: 'Car insurance', when: 'in 8 days',  date: '22 Mar', amount: 128.00, icon: 'shield' },
    { id: 'u2', name: 'Visa Signature', when: 'in 19 days', date: '2 Apr',  amount: 1247.80, icon: 'credit-card', warn: true },
    { id: 'u3', name: 'Rent',          when: 'in 18 days', date: '1 Apr',  amount: 1850.00, icon: 'house' },
  ];

  // Daily net position across the month, for the Today strip.
  const trend = [4820, 4790, 2940, 2880, 2870, 2810, 2795, 2740, 2690, 5290, 5194, 4894, 4880, 4183];

  // ── Looking ahead ───────────────────────────────────────────────────────────
  // Everything here is an assumption, and the screen says so. Base months repeat
  // the recurring facts we already know; one-offs are things you've told us about.
  const forecast = {
    start: { label: '14 Mar', balance: checking },
    base: { income: 5200, bills: 2265, everyday: 1070, saving: 700 },
    // What a month is actually made of. The drill-down scales these to whatever a
    // given month's total is, so opening any bar shows lines, not a mystery number.
    lines: {
      income: [
        { label: 'Bridgetown Studio', detail: 'Paycheque · 10th', amount: 2600, icon: 'briefcase' },
        { label: 'Bridgetown Studio', detail: 'Paycheque · 25th', amount: 2600, icon: 'briefcase' },
      ],
      bills: [
        { label: 'Rent',          detail: 'Salmon Creek Rentals · 1st', amount: 1850, icon: 'house' },
        { label: 'Electric',      detail: 'Pacific Power · 10th',       amount: 110,  icon: 'zap' },
        { label: 'Internet',      detail: 'Ziply Fiber · 4th',          amount: 70,   icon: 'wifi' },
        { label: 'Phone',         detail: 'Verizon · 6th',              amount: 45,   icon: 'smartphone' },
        { label: 'Car insurance', detail: 'Every month · 22nd',         amount: 128,  icon: 'shield' },
        { label: 'Subscriptions', detail: 'Seven of them',              amount: 62,   icon: 'repeat' },
      ],
      everyday: [
        { label: 'Groceries',  detail: 'Six-month average', amount: 470, icon: 'shopping-basket' },
        { label: 'Eating out', detail: 'Six-month average', amount: 235, icon: 'utensils' },
        { label: 'Fun',        detail: 'Six-month average', amount: 175, icon: 'ticket' },
        { label: 'Transport',  detail: 'Six-month average', amount: 95,  icon: 'train-front' },
        { label: 'Health',     detail: 'Six-month average', amount: 95,  icon: 'heart-pulse' },
      ],
    },
    // Six months of what actually happened, so the two bars have a past as well as
    // a future. `swept` is the leftover that went into savings at month end — it is
    // why checking stays flat instead of quietly ballooning.
    past: [
      { key: '2025-09', label: 'September', short: 'Sep', income: 5200, bills: 2210, everyday: 1182, saving: 700, committed: 168, swept: 900 },
      { key: '2025-10', label: 'October',   short: 'Oct', income: 5200, bills: 2265, everyday: 1294, saving: 700, committed: 168, swept: 750 },
      { key: '2025-11', label: 'November',  short: 'Nov', income: 5850, bills: 2265, everyday: 1418, saving: 700, committed: 267, swept: 1000, note: 'A freelance job on top of the paycheques' },
      { key: '2025-12', label: 'December',  short: 'Dec', income: 5200, bills: 2312, everyday: 1684, saving: 400, committed: 267, swept: 700, note: 'Gifts, and the flights deposit' },
      { key: '2026-01', label: 'January',   short: 'Jan', income: 5200, bills: 2288, everyday: 986,  saving: 700, committed: 267, swept: 900 },
      { key: '2026-02', label: 'February',  short: 'Feb', income: 5200, bills: 2255, everyday: 1108, saving: 700, committed: 219, swept: 1000 },
    ],
    months: [
      { key: '2026-03', label: 'Rest of Mar', short: 'Mar', partial: true, income: 2600, bills: 1640, everyday: 480, saving: 300 },
      { key: '2026-04', label: 'April',    short: 'Apr' },
      { key: '2026-05', label: 'May',      short: 'May' },
      { key: '2026-06', label: 'June',     short: 'Jun', oneOff: { label: 'Japan flights', amount: -1450, tag: 'japan' } },
      { key: '2026-07', label: 'July',     short: 'Jul' },
      { key: '2026-08', label: 'August',   short: 'Aug', oneOff: { label: 'Car registration', amount: -180 } },
      { key: '2026-09', label: 'September', short: 'Sep' },
      { key: '2026-10', label: 'October',  short: 'Oct' },
      { key: '2026-11', label: 'November', short: 'Nov', oneOff: { label: 'Japan trip', amount: -2400, tag: 'japan' } },
      { key: '2026-12', label: 'December', short: 'Dec', oneOff: { label: 'Christmas', amount: -600 } },
      { key: '2027-01', label: 'January',  short: 'Jan' },
      { key: '2027-02', label: 'February', short: 'Feb', oneOff: { label: 'Car insurance renewal', amount: -640 } },
      { key: '2027-03', label: 'March',    short: 'Mar' },
    ],
    // Assumptions the user can argue with, shown as editable inputs.
    inputs: [
      { id: 'income',   label: 'Take-home each month', value: 5200, hint: 'Two paycheques from Bridgetown Studio' },
      { id: 'bills',    label: 'Bills each month',     value: 2265, hint: 'Rent, power, internet, phone, insurance, subs' },
      { id: 'everyday', label: 'Everyday spending',    value: 1070, hint: 'Your six-month average, not your plan' },
      { id: 'saving',   label: 'Set aside for goals',  value: 700,  hint: 'Japan trip $300 · Emergency fund $400' },
    ],
    goals: [
      { id: 'g1', name: 'Japan trip',     icon: 'plane',    saved: 1905, goal: 4800,  monthly: 300 },
      { id: 'g2', name: 'Emergency fund', icon: 'umbrella', saved: 8400, goal: 12000, monthly: 400 },
    ],
  };

  // ── Already committed ───────────────────────────────────────────────────────
  // Buy-now-pay-later and anything else with a payment schedule. These are the
  // bills you've already agreed to but haven't paid yet, so the forecast has to
  // know about them or it's lying.
  const installments = [
    { id: 'i1', app: 'Cashea', merchant: 'Farmatodo',   each: 24.00, paidCount: 1, count: 4,  next: '2026-03-20', every: 'fortnight' },
    { id: 'i2', app: 'Cashea', merchant: 'Traki',       each: 45.00, paidCount: 2, count: 4,  next: '2026-03-26', every: 'fortnight' },
    { id: 'i3', app: 'Cashea', merchant: 'Movistar',    each: 18.50, paidCount: 0, count: 3,  next: '2026-04-04', every: 'fortnight' },
    { id: 'i4', app: 'Affirm', merchant: 'Peloton',     each: 99.00, paidCount: 5, count: 12, next: '2026-04-02', every: 'month' },
  ];

  const installmentApps = ['Cashea', 'Affirm', 'Klarna', 'Afterpay', 'PayPal Pay in 4', 'Store plan'];

  // Expand plans into the payments still to come. Called once for the fixtures, and
  // again whenever the user adds a plan, so a new Cashea plan lands in the forecast.
  const expand = (plans) => {
    const schedule = [];
    plans.forEach((p) => {
      const left = p.count - p.paidCount;
      let d = parse(p.next);
      for (let n = 0; n < left; n++) {
        schedule.push({
          id: p.id + '-' + n, planId: p.id, app: p.app, merchant: p.merchant, amount: p.each,
          date: d.toISOString().slice(0, 10), mkey: d.toISOString().slice(0, 7),
          n: p.paidCount + n + 1, of: p.count,
          label: d.getDate() + ' ' + SHORT[d.getMonth()],
        });
        d = new Date(d);
        if (p.every === 'fortnight') d.setDate(d.getDate() + 14);
        else d.setMonth(d.getMonth() + 1);
      }
    });
    schedule.sort((a, b) => (a.date < b.date ? -1 : 1));
    return {
      schedule,
      byMonth: schedule.reduce((acc, s) => { acc[s.mkey] = (acc[s.mkey] || 0) + s.amount; return acc; }, {}),
      total: schedule.reduce((s, p) => s + p.amount, 0),
    };
  };

  const base = expand(installments);
  const schedule = base.schedule;
  const committedByMonth = base.byMonth;
  const committedTotal = base.total;

  // ── Rules ───────────────────────────────────────────────────────────────────
  // Every "always sort this way" switch makes one of these. They have to live
  // somewhere the user can find them, or the switch is a trapdoor.
  const rules = [
    { id: 'r1', match: 'Sunrise Market',    cat: 'groceries', matched: 14, since: 'Since 4 Jan',      mode: 'auto' },
    { id: 'r2', match: 'Kinto Coffee',      cat: 'dining',    matched: 23, since: 'Since 12 Nov',     mode: 'auto' },
    { id: 'r3', match: 'TRIMET*',           cat: 'transport', matched: 9,  since: 'Since 2 Feb',      mode: 'auto', note: 'Anything starting with TRIMET' },
    { id: 'r4', match: 'Netflix',           cat: 'subs',      matched: 12, since: 'Since 1 Apr 2025', mode: 'auto' },
    { id: 'r5', match: 'Bridgetown Studio', cat: 'income',    matched: 11, since: 'Since 1 Apr 2025', mode: 'auto' },
    { id: 'r6', match: 'Aurora Health',     cat: 'health',    matched: 4,  since: 'Since 18 Sep',     mode: 'ask', note: 'Asks first — the amounts vary' },
  ];

  // What the app is allowed to interrupt you about. Two on by default; the rest
  // are off, because an app that can be finished shouldn't nag.
  const notices = [
    { id: 'n1', label: 'A plan went over',                   desc: 'The moment it happens',                    on: true },
    { id: 'n2', label: 'A bill is due in 3 days',            desc: "Only if the money isn't set aside yet",     on: false },
    { id: 'n3', label: 'A charge is bigger than usual',      desc: 'Compared with what you normally pay there', on: false },
    { id: 'n4', label: 'Balance dropped below your cushion', desc: 'Currently $1,500',                          on: false },
    { id: 'n5', label: "Weekly: here's what you spent",      desc: 'Monday morning',                            on: true },
    { id: 'n6', label: 'A subscription price went up',       desc: 'We compare each charge with the last one',  on: false },
  ];

  // ── Year in review ──────────────────────────────────────────────────────────
  const year = {
    label: 'Last 12 months',
    range: 'March 2025 – February 2026',
    in: 62400, out: 46188, saved: 8400,
    months: [3620, 3410, 3980, 4120, 3540, 3760, 3890, 4310, 5240, 3480, 3210, 3628],
    monthLabels: ['Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb'],
    categories: [
      { cat: 'home',      amount: 22200 },
      { cat: 'groceries', amount: 6840 },
      { cat: 'dining',    amount: 4310 },
      { cat: 'utilities', amount: 3520 },
      { cat: 'fun',       amount: 2960 },
      { cat: 'transport', amount: 1740 },
      { cat: 'health',    amount: 1420 },
      { cat: 'subs',      amount: 744 },
    ],
    biggest: { month: 'December', amount: 5240, why: 'flights and gifts' },
    quietest: { month: 'January', amount: 3210 },
    subs: { count: 7, amount: 744 },
    merchant: { name: 'Sunrise Market', visits: 96, amount: 5820 },
    changed: 'You spent $2,140 less on eating out than the year before.',
  };

  return {
    money, checking, billsDue, assigned, safeToSpend, accounts, spendAccounts, netWorth,
    tx, CAT, CAT_KEYS, plans, upcoming, trend, forecast, enrich, rules, notices, year,
    installments, schedule, committedByMonth, committedTotal, expand, installmentApps,
    months, weeks, weekLabel, MONTHS, SHORT,
  };
})();
