// Bodega preview runtime — generated, do not hand-edit.
//
// Each component source under components/ is wrapped in its own IIFE and its
// exports hung off window.Bodega, so the design-system cards and UI kits render
// without a bundler. The .jsx files remain the source of truth; ES imports are
// resolved here by reading earlier registrations off the same namespace.

window.Bodega = window.Bodega || {};

// ── Icon ──────────────────────────────────────────
;(function () {
  function toPascal(name) {
    return String(name)
      .split(/[-_\s]+/)
      .filter(Boolean)
      .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
      .join("");
  }

  /**
   * Thin wrapper over the Lucide icon set (loaded from CDN).
   * Bodega ships no drawn iconography of its own — see readme.md ▸ ICONOGRAPHY.
   */
  function Icon({ name, size = 16, strokeWidth = 1.75, color = "currentColor", style, ...rest }) {
    const set = (typeof window !== "undefined" && window.lucide && window.lucide.icons) || {};
    const node = set[toPascal(name)] || set[name];

    // Lucide ships each icon as ["svg", attrs, children]; older builds hand back a
    // flat list of child tuples. Accept either and render only the children.
    let parts = [];
    if (Array.isArray(node)) {
      parts = typeof node[0] === "string" && Array.isArray(node[2]) ? node[2] : node;
    }

    return (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
        focusable="false"
        style={{ display: "block", flex: "0 0 auto", ...style }}
        {...rest}
      >
        {parts.map((p, i) => (Array.isArray(p) ? React.createElement(p[0], { key: i, ...p[1] }) : null))}
      </svg>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Icon });
})();

// ── Button ────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const SIZES = {
    sm: { height: "var(--control-sm)", padding: "0 10px", font: "var(--body-sm-size)", gap: 5, icon: 14, radius: "var(--radius-sm)" },
    md: { height: "var(--control-md)", padding: "0 13px", font: "var(--body-size)", gap: 6, icon: 16, radius: "var(--radius-md)" },
    lg: { height: "var(--control-lg)", padding: "0 18px", font: "var(--body-lg-size)", gap: 8, icon: 18, radius: "var(--radius-md)" },
  };

  const VARIANTS = {
    primary: {
      rest: { background: "var(--surface-accent)", color: "var(--text-inverse)", border: "1px solid var(--border-accent)", boxShadow: "var(--shadow-accent)" },
      hover: { background: "var(--surface-accent-hover)" },
      active: { background: "var(--surface-accent-active)", boxShadow: "var(--shadow-press)" },
    },
    secondary: {
      rest: { background: "var(--surface-raised)", color: "var(--text-primary)", border: "1px solid var(--border-default)", boxShadow: "var(--shadow-xs)" },
      hover: { background: "var(--paper-050)", borderColor: "var(--border-strong)" },
      active: { background: "var(--paper-200)", boxShadow: "var(--shadow-press)" },
    },
    ghost: {
      rest: { background: "transparent", color: "var(--text-secondary)", border: "1px solid transparent", boxShadow: "none" },
      hover: { background: "var(--surface-hover)", color: "var(--text-primary)" },
      active: { background: "var(--surface-active)" },
    },
    danger: {
      rest: { background: "var(--clay-600)", color: "#fff", border: "1px solid var(--clay-700)", boxShadow: "var(--shadow-accent)" },
      hover: { background: "var(--clay-700)" },
      active: { background: "var(--red-800)", boxShadow: "var(--shadow-press)" },
    },
    link: {
      rest: { background: "transparent", color: "var(--text-accent)", border: "1px solid transparent", boxShadow: "none", padding: 0, height: "auto" },
      hover: { color: "var(--red-700)", textDecoration: "underline", textUnderlineOffset: 2 },
      active: { color: "var(--red-800)" },
    },
  };

  function Button({
    variant = "secondary",
    size = "md",
    iconStart,
    iconEnd,
    loading = false,
    disabled = false,
    fullWidth = false,
    type = "button",
    style,
    children,
    ...rest
  }) {
    const [hover, setHover] = React.useState(false);
    const [press, setPress] = React.useState(false);
    const s = SIZES[size] || SIZES.md;
    const v = VARIANTS[variant] || VARIANTS.secondary;
    const off = disabled || loading;

    const merged = {
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      gap: s.gap,
      height: s.height,
      padding: s.padding,
      width: fullWidth ? "100%" : undefined,
      fontFamily: "var(--font-sans)",
      fontSize: s.font,
      fontWeight: "var(--weight-medium)",
      letterSpacing: "-0.004em",
      lineHeight: 1,
      borderRadius: s.radius,
      cursor: off ? "not-allowed" : "pointer",
      transition: "var(--transition-control), transform var(--dur-instant) var(--ease-standard)",
      whiteSpace: "nowrap",
      transform: press && !off ? "translateY(0.5px)" : "none",
      opacity: off ? 0.5 : 1,
      ...v.rest,
      ...(hover && !off ? v.hover : null),
      ...(press && !off ? v.active : null),
      ...style,
    };

    return (
      <button
        type={type}
        disabled={off}
        style={merged}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => { setHover(false); setPress(false); }}
        onMouseDown={() => setPress(true)}
        onMouseUp={() => setPress(false)}
        {...rest}
      >
        {loading ? <Spinner size={s.icon} /> : iconStart ? <Icon name={iconStart} size={s.icon} /> : null}
        {children}
        {iconEnd && !loading ? <Icon name={iconEnd} size={s.icon} /> : null}
      </button>
    );
  }

  function Spinner({ size }) {
    return (
      <span
        style={{
          width: size - 2,
          height: size - 2,
          borderRadius: "50%",
          border: "1.75px solid currentColor",
          borderTopColor: "transparent",
          opacity: 0.85,
          animation: "bodega-spin 620ms linear infinite",
        }}
      />
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Button });
})();

// ── IconButton ────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const SIZES = { sm: { box: 26, icon: 14 }, md: { box: 32, icon: 16 }, lg: { box: 40, icon: 18 } };

  const VARIANTS = {
    ghost: { rest: { background: "transparent", color: "var(--text-secondary)", border: "1px solid transparent" }, hover: { background: "var(--surface-hover)", color: "var(--text-primary)" }, active: { background: "var(--surface-active)" } },
    secondary: { rest: { background: "var(--surface-raised)", color: "var(--text-primary)", border: "1px solid var(--border-default)", boxShadow: "var(--shadow-xs)" }, hover: { background: "var(--paper-050)", borderColor: "var(--border-strong)" }, active: { background: "var(--paper-200)", boxShadow: "var(--shadow-press)" } },
    primary: { rest: { background: "var(--surface-accent)", color: "var(--text-inverse)", border: "1px solid var(--border-accent)", boxShadow: "var(--shadow-accent)" }, hover: { background: "var(--surface-accent-hover)" }, active: { background: "var(--surface-accent-active)", boxShadow: "var(--shadow-press)" } },
  };

  function IconButton({ icon, label, variant = "ghost", size = "md", selected = false, disabled = false, style, ...rest }) {
    const [hover, setHover] = React.useState(false);
    const [press, setPress] = React.useState(false);
    const s = SIZES[size] || SIZES.md;
    const v = VARIANTS[variant] || VARIANTS.ghost;

    return (
      <button
        type="button"
        aria-label={label}
        title={label}
        disabled={disabled}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => { setHover(false); setPress(false); }}
        onMouseDown={() => setPress(true)}
        onMouseUp={() => setPress(false)}
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: s.box,
          height: s.box,
          borderRadius: "var(--radius-sm)",
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.45 : 1,
          transition: "var(--transition-control)",
          ...v.rest,
          ...(selected ? { background: "var(--surface-selected)", color: "var(--text-accent)" } : null),
          ...(hover && !disabled ? v.hover : null),
          ...(press && !disabled ? v.active : null),
          ...style,
        }}
        {...rest}
      >
        <Icon name={icon} size={s.icon} />
      </button>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { IconButton });
})();

// ── Input ─────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const SIZES = { sm: "var(--control-sm)", md: "var(--control-md)", lg: "var(--control-lg)" };

  function FieldShell({ label, hint, error, required, htmlFor, children, style }) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 5, ...style }}>
        {label ? (
          <label
            htmlFor={htmlFor}
            style={{ fontSize: "var(--body-sm-size)", fontWeight: "var(--weight-medium)", color: "var(--text-primary)", letterSpacing: "-0.004em" }}
          >
            {label}
            {required ? <span style={{ color: "var(--clay-600)", marginLeft: 3 }}>*</span> : null}
          </label>
        ) : null}
        {children}
        {error ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-danger)" }}>{error}</span>
        ) : hint ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-tertiary)" }}>{hint}</span>
        ) : null}
      </div>
    );
  }

  function Input({
    label, hint, error, required, size = "md", iconStart, suffix, prefix,
    id, disabled = false, style, containerStyle, ...rest
  }) {
    const [focus, setFocus] = React.useState(false);
    const [hover, setHover] = React.useState(false);
    const auto = React.useId ? React.useId() : "bodega-input";
    const inputId = id || auto;

    const border = error ? "var(--clay-600)" : focus ? "var(--red-500)" : hover ? "var(--border-strong)" : "var(--border-default)";

    return (
      <FieldShell label={label} hint={hint} error={error} required={required} htmlFor={inputId} style={containerStyle}>
        <div
          onMouseEnter={() => setHover(true)}
          onMouseLeave={() => setHover(false)}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 7,
            height: SIZES[size] || SIZES.md,
            padding: "0 10px",
            background: disabled ? "var(--surface-sunken)" : "var(--surface-raised)",
            border: `1px solid ${border}`,
            borderRadius: "var(--radius-sm)",
            boxShadow: focus ? (error ? "0 0 0 3px rgba(176,58,43,0.18)" : "var(--focus-ring-flush)") : "var(--shadow-xs)",
            transition: "var(--transition-control)",
            opacity: disabled ? 0.6 : 1,
          }}
        >
          {iconStart ? <Icon name={iconStart} size={15} color="var(--text-tertiary)" /> : null}
          {prefix ? <span style={{ color: "var(--text-tertiary)", fontSize: "var(--body-size)", whiteSpace: "nowrap" }}>{prefix}</span> : null}
          <input
            id={inputId}
            disabled={disabled}
            onFocus={(e) => { setFocus(true); rest.onFocus && rest.onFocus(e); }}
            onBlur={(e) => { setFocus(false); rest.onBlur && rest.onBlur(e); }}
            {...rest}
            style={{
              flex: 1,
              minWidth: 0,
              border: "none",
              outline: "none",
              background: "transparent",
              fontFamily: "var(--font-sans)",
              fontSize: "var(--body-size)",
              color: "var(--text-primary)",
              padding: 0,
              ...style,
            }}
          />
          {suffix ? <span style={{ color: "var(--text-tertiary)", fontSize: "var(--caption-size)", whiteSpace: "nowrap" }}>{suffix}</span> : null}
        </div>
      </FieldShell>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Input });
})();

// ── TextArea ──────────────────────────────────────
;(function () {
  function FieldShell({ label, hint, error, required, htmlFor, children, style }) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 5, ...style }}>
        {label ? (
          <label
            htmlFor={htmlFor}
            style={{ fontSize: "var(--body-sm-size)", fontWeight: "var(--weight-medium)", color: "var(--text-primary)", letterSpacing: "-0.004em" }}
          >
            {label}
            {required ? <span style={{ color: "var(--clay-600)", marginLeft: 3 }}>*</span> : null}
          </label>
        ) : null}
        {children}
        {error ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-danger)" }}>{error}</span>
        ) : hint ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-tertiary)" }}>{hint}</span>
        ) : null}
      </div>
    );
  }

  function TextArea({ label, hint, error, required, rows = 4, id, disabled = false, style, containerStyle, ...rest }) {
    const [focus, setFocus] = React.useState(false);
    const auto = React.useId ? React.useId() : "bodega-textarea";
    const areaId = id || auto;
    const border = error ? "var(--clay-600)" : focus ? "var(--red-500)" : "var(--border-default)";

    return (
      <FieldShell label={label} hint={hint} error={error} required={required} htmlFor={areaId} style={containerStyle}>
        <textarea
          id={areaId}
          rows={rows}
          disabled={disabled}
          onFocus={(e) => { setFocus(true); rest.onFocus && rest.onFocus(e); }}
          onBlur={(e) => { setFocus(false); rest.onBlur && rest.onBlur(e); }}
          {...rest}
          style={{
            width: "100%",
            padding: "9px 11px",
            background: disabled ? "var(--surface-sunken)" : "var(--surface-raised)",
            border: `1px solid ${border}`,
            borderRadius: "var(--radius-sm)",
            boxShadow: focus ? (error ? "0 0 0 3px rgba(176,58,43,0.18)" : "var(--focus-ring-flush)") : "var(--shadow-xs)",
            fontFamily: "var(--font-sans)",
            fontSize: "var(--body-size)",
            lineHeight: "var(--body-line)",
            color: "var(--text-primary)",
            outline: "none",
            resize: "vertical",
            transition: "var(--transition-control)",
            ...style,
          }}
        />
      </FieldShell>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { TextArea });
})();

// ── Select ────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const SIZES = { sm: "var(--control-sm)", md: "var(--control-md)", lg: "var(--control-lg)" };

  function FieldShell({ label, hint, error, required, htmlFor, children, style }) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 5, ...style }}>
        {label ? (
          <label
            htmlFor={htmlFor}
            style={{ fontSize: "var(--body-sm-size)", fontWeight: "var(--weight-medium)", color: "var(--text-primary)", letterSpacing: "-0.004em" }}
          >
            {label}
            {required ? <span style={{ color: "var(--clay-600)", marginLeft: 3 }}>*</span> : null}
          </label>
        ) : null}
        {children}
        {error ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-danger)" }}>{error}</span>
        ) : hint ? (
          <span style={{ fontSize: "var(--caption-size)", lineHeight: "var(--caption-line)", color: "var(--text-tertiary)" }}>{hint}</span>
        ) : null}
      </div>
    );
  }

  function Select({ label, hint, error, required, size = "md", options = [], placeholder, id, disabled = false, style, containerStyle, ...rest }) {
    const [focus, setFocus] = React.useState(false);
    const [hover, setHover] = React.useState(false);
    const auto = React.useId ? React.useId() : "bodega-select";
    const selectId = id || auto;
    const border = error ? "var(--clay-600)" : focus ? "var(--red-500)" : hover ? "var(--border-strong)" : "var(--border-default)";

    return (
      <FieldShell label={label} hint={hint} error={error} required={required} htmlFor={selectId} style={containerStyle}>
        <div
          onMouseEnter={() => setHover(true)}
          onMouseLeave={() => setHover(false)}
          style={{
            position: "relative",
            display: "flex",
            alignItems: "center",
            height: SIZES[size] || SIZES.md,
            background: disabled ? "var(--surface-sunken)" : "var(--surface-raised)",
            border: `1px solid ${border}`,
            borderRadius: "var(--radius-sm)",
            boxShadow: focus ? "var(--focus-ring-flush)" : "var(--shadow-xs)",
            transition: "var(--transition-control)",
            opacity: disabled ? 0.6 : 1,
          }}
        >
          <select
            id={selectId}
            disabled={disabled}
            onFocus={() => setFocus(true)}
            onBlur={() => setFocus(false)}
            {...rest}
            style={{
              appearance: "none",
              WebkitAppearance: "none",
              width: "100%",
              height: "100%",
              border: "none",
              outline: "none",
              background: "transparent",
              padding: "0 30px 0 10px",
              fontFamily: "var(--font-sans)",
              fontSize: "var(--body-size)",
              color: "var(--text-primary)",
              cursor: disabled ? "not-allowed" : "pointer",
              ...style,
            }}
          >
            {placeholder ? <option value="">{placeholder}</option> : null}
            {options.map((o) => {
              const opt = typeof o === "string" ? { value: o, label: o } : o;
              return (
                <option key={opt.value} value={opt.value} disabled={opt.disabled}>
                  {opt.label}
                </option>
              );
            })}
          </select>
          <span style={{ position: "absolute", right: 9, pointerEvents: "none", display: "flex" }}>
            <Icon name="chevron-down" size={14} color="var(--text-tertiary)" />
          </span>
        </div>
      </FieldShell>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Select });
})();

// ── Checkbox ──────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function Checkbox({ label, description, checked, indeterminate = false, disabled = false, onChange, id, style, ...rest }) {
    const [hover, setHover] = React.useState(false);
    const auto = React.useId ? React.useId() : "bodega-checkbox";
    const boxId = id || auto;
    const on = checked || indeterminate;

    return (
      <label
        htmlFor={boxId}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
        style={{
          display: "flex",
          alignItems: description ? "flex-start" : "center",
          gap: 9,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1,
          ...style,
        }}
      >
        <input id={boxId} type="checkbox" checked={!!checked} disabled={disabled} onChange={onChange} {...rest}
          style={{ position: "absolute", opacity: 0, width: 0, height: 0 }} />
        <span
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: 16,
            height: 16,
            flex: "0 0 auto",
            marginTop: description ? 1 : 0,
            borderRadius: "var(--radius-xs)",
            background: on ? "var(--surface-accent)" : "var(--surface-raised)",
            border: on ? "1px solid var(--border-accent)" : `1px solid ${hover && !disabled ? "var(--border-strong)" : "var(--border-default)"}`,
            boxShadow: on ? "var(--shadow-accent)" : "var(--shadow-xs)",
            transition: "var(--transition-control)",
            color: "#fff",
          }}
        >
          {indeterminate ? (
            <span style={{ width: 8, height: 1.75, background: "currentColor", borderRadius: 1 }} />
          ) : checked ? (
            <Icon name="check" size={12} strokeWidth={2.75} />
          ) : null}
        </span>
        {label ? (
          <span style={{ display: "flex", flexDirection: "column", gap: 1 }}>
            <span style={{ fontSize: "var(--body-size)", color: "var(--text-primary)", lineHeight: 1.35 }}>{label}</span>
            {description ? (
              <span style={{ fontSize: "var(--caption-size)", color: "var(--text-tertiary)", lineHeight: "var(--caption-line)" }}>{description}</span>
            ) : null}
          </span>
        ) : null}
      </label>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Checkbox });
})();

// ── Radio ─────────────────────────────────────────
;(function () {
  function Radio({ label, description, checked, disabled = false, name, value, onChange, id, style, ...rest }) {
    const [hover, setHover] = React.useState(false);
    const auto = React.useId ? React.useId() : "bodega-radio";
    const radioId = id || auto;

    return (
      <label
        htmlFor={radioId}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
        style={{
          display: "flex",
          alignItems: description ? "flex-start" : "center",
          gap: 9,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1,
          ...style,
        }}
      >
        <input id={radioId} type="radio" name={name} value={value} checked={!!checked} disabled={disabled} onChange={onChange} {...rest}
          style={{ position: "absolute", opacity: 0, width: 0, height: 0 }} />
        <span
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: 16,
            height: 16,
            flex: "0 0 auto",
            marginTop: description ? 1 : 0,
            borderRadius: "50%",
            background: "var(--surface-raised)",
            border: checked ? "5px solid var(--surface-accent)" : `1px solid ${hover && !disabled ? "var(--border-strong)" : "var(--border-default)"}`,
            boxShadow: "var(--shadow-xs)",
            transition: "var(--transition-control)",
          }}
        />
        {label ? (
          <span style={{ display: "flex", flexDirection: "column", gap: 1 }}>
            <span style={{ fontSize: "var(--body-size)", color: "var(--text-primary)", lineHeight: 1.35 }}>{label}</span>
            {description ? (
              <span style={{ fontSize: "var(--caption-size)", color: "var(--text-tertiary)", lineHeight: "var(--caption-line)" }}>{description}</span>
            ) : null}
          </span>
        ) : null}
      </label>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Radio });
})();

// ── Switch ────────────────────────────────────────
;(function () {
  function Switch({ label, description, checked = false, disabled = false, onChange, id, style, ...rest }) {
    const auto = React.useId ? React.useId() : "bodega-switch";
    const switchId = id || auto;

    return (
      <label
        htmlFor={switchId}
        style={{
          display: "flex",
          alignItems: description ? "flex-start" : "center",
          gap: 10,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1,
          ...style,
        }}
      >
        <input id={switchId} type="checkbox" role="switch" checked={checked} disabled={disabled} onChange={onChange} {...rest}
          style={{ position: "absolute", opacity: 0, width: 0, height: 0 }} />
        <span
          style={{
            position: "relative",
            width: 30,
            height: 18,
            flex: "0 0 auto",
            marginTop: description ? 1 : 0,
            borderRadius: "var(--radius-pill)",
            background: checked ? "var(--surface-accent)" : "var(--ink-200)",
            border: checked ? "1px solid var(--border-accent)" : "1px solid var(--border-default)",
            boxShadow: "inset 0 1px 2px rgba(23,21,15,0.08)",
            transition: "background-color var(--dur-fast) var(--ease-standard), border-color var(--dur-fast) var(--ease-standard)",
          }}
        >
          <span
            style={{
              position: "absolute",
              top: 1,
              left: checked ? 13 : 1,
              width: 14,
              height: 14,
              borderRadius: "50%",
              background: "var(--paper-000)",
              boxShadow: "0 1px 2px rgba(23,21,15,0.28)",
              transition: "left var(--dur-fast) var(--ease-lift)",
            }}
          />
        </span>
        {label ? (
          <span style={{ display: "flex", flexDirection: "column", gap: 1 }}>
            <span style={{ fontSize: "var(--body-size)", color: "var(--text-primary)", lineHeight: 1.35 }}>{label}</span>
            {description ? (
              <span style={{ fontSize: "var(--caption-size)", color: "var(--text-tertiary)", lineHeight: "var(--caption-line)" }}>{description}</span>
            ) : null}
          </span>
        ) : null}
      </label>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Switch });
})();

// ── Card ──────────────────────────────────────────
;(function () {
  const PADS = { none: 0, sm: "var(--space-3)", md: "var(--space-5)", lg: "var(--space-6)" };

  function Card({ title, subtitle, actions, footer, padding = "md", interactive = false, tone = "raised", style, bodyStyle, children, ...rest }) {
    const [hover, setHover] = React.useState(false);
    const pad = PADS[padding] !== undefined ? PADS[padding] : PADS.md;

    const tones = {
      raised: { background: "var(--surface-raised)", border: "1px solid var(--border-subtle)", boxShadow: "var(--shadow-sm)" },
      flat: { background: "var(--surface-raised)", border: "1px solid var(--border-default)", boxShadow: "none" },
      sunken: { background: "var(--surface-sunken)", border: "1px solid var(--border-subtle)", boxShadow: "none" },
      accent: { background: "var(--thyme-050)", border: "1px solid var(--thyme-100)", boxShadow: "none" },
    };

    return (
      <section
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
        style={{
          display: "flex",
          flexDirection: "column",
          borderRadius: "var(--radius-lg)",
          overflow: "hidden",
          transition: "box-shadow var(--dur-fast) var(--ease-standard), border-color var(--dur-fast) var(--ease-standard), transform var(--dur-fast) var(--ease-standard)",
          cursor: interactive ? "pointer" : undefined,
          ...tones[tone],
          ...(interactive && hover ? { boxShadow: "var(--shadow-md)", borderColor: "var(--border-default)", transform: "translateY(-1px)" } : null),
          ...style,
        }}
        {...rest}
      >
        {title || actions ? (
          <header
            style={{
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
              gap: "var(--space-4)",
              padding: `${pad === 0 ? "var(--space-4)" : pad} ${pad === 0 ? "var(--space-4)" : pad} 0`,
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
              {title ? (
                <h3 style={{ margin: 0, fontFamily: "var(--font-sans)", fontSize: "var(--title-3-size)", lineHeight: "var(--title-3-line)", letterSpacing: "var(--title-3-tracking)", fontWeight: "var(--weight-semibold)", color: "var(--text-primary)" }}>
                  {title}
                </h3>
              ) : null}
              {subtitle ? (
                <p style={{ margin: 0, fontSize: "var(--body-sm-size)", lineHeight: "var(--body-sm-line)", color: "var(--text-tertiary)" }}>{subtitle}</p>
              ) : null}
            </div>
            {actions ? <div style={{ display: "flex", alignItems: "center", gap: "var(--space-2)", flex: "0 0 auto" }}>{actions}</div> : null}
          </header>
        ) : null}
        <div style={{ padding: pad, ...bodyStyle }}>{children}</div>
        {footer ? (
          <footer style={{ padding: `var(--space-3) ${pad === 0 ? "var(--space-4)" : pad}`, borderTop: "1px solid var(--border-subtle)", background: "var(--paper-050)" }}>
            {footer}
          </footer>
        ) : null}
      </section>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Card });
})();

// ── Badge ─────────────────────────────────────────
;(function () {
  const TONES = {
    neutral: { bg: "var(--status-neutral-bg)", bd: "var(--status-neutral-border)", fg: "var(--status-neutral-text)", dot: "var(--ink-400)" },
    success: { bg: "var(--status-success-bg)", bd: "var(--status-success-border)", fg: "var(--status-success-text)", dot: "var(--thyme-500)" },
    warning: { bg: "var(--status-warning-bg)", bd: "var(--status-warning-border)", fg: "var(--status-warning-text)", dot: "var(--ochre-500)" },
    danger: { bg: "var(--status-danger-bg)", bd: "var(--status-danger-border)", fg: "var(--status-danger-text)", dot: "var(--clay-600)" },
    info: { bg: "var(--status-info-bg)", bd: "var(--status-info-border)", fg: "var(--status-info-text)", dot: "var(--slate-600)" },
  };

  function Badge({ tone = "neutral", dot = false, size = "md", style, children, ...rest }) {
    const t = TONES[tone] || TONES.neutral;
    const sm = size === "sm";
    return (
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 5,
          height: sm ? 18 : 22,
          padding: sm ? "0 6px" : "0 8px",
          borderRadius: "var(--radius-xs)",
          background: t.bg,
          border: `1px solid ${t.bd}`,
          color: t.fg,
          fontFamily: "var(--font-mono)",
          fontSize: sm ? 10 : 10.5,
          textTransform: "uppercase",
          letterSpacing: "0.05em",
          fontWeight: "var(--weight-medium)",
          lineHeight: 1,
          whiteSpace: "nowrap",
          ...style,
        }}
        {...rest}
      >
        {dot ? <span style={{ width: 5, height: 5, borderRadius: "50%", background: t.dot, flex: "0 0 auto" }} /> : null}
        {children}
      </span>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Badge });
})();

// ── Tag ───────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function Tag({ onRemove, onClick, selected = false, icon, style, children, ...rest }) {
    const [hover, setHover] = React.useState(false);
    const clickable = !!onClick;

    return (
      <span
        onClick={onClick}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 5,
          height: 24,
          padding: onRemove ? "0 4px 0 9px" : "0 9px",
          borderRadius: "var(--radius-pill)",
          background: selected ? "var(--thyme-100)" : hover && clickable ? "var(--paper-200)" : "var(--paper-100)",
          border: `1px solid ${selected ? "var(--thyme-300)" : "var(--border-default)"}`,
          color: selected ? "var(--thyme-800)" : "var(--text-secondary)",
          fontFamily: "var(--font-mono)",
          fontSize: 11,
          letterSpacing: "0.04em",
          fontWeight: "var(--weight-medium)",
          lineHeight: 1,
          cursor: clickable ? "pointer" : "default",
          transition: "var(--transition-control)",
          whiteSpace: "nowrap",
          ...style,
        }}
        {...rest}
      >
        {icon ? <Icon name={icon} size={12} /> : null}
        {children}
        {onRemove ? (
          <button
            type="button"
            aria-label="Remove"
            onClick={(e) => { e.stopPropagation(); onRemove(e); }}
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              width: 16,
              height: 16,
              marginLeft: 1,
              border: "none",
              borderRadius: "50%",
              background: "transparent",
              color: "inherit",
              cursor: "pointer",
              opacity: 0.65,
            }}
          >
            <Icon name="x" size={11} strokeWidth={2.25} />
          </button>
        ) : null}
      </span>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Tag });
})();

// ── StatTile ──────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function StatTile({ label, value, delta, deltaDirection, caption, sparkline, style, ...rest }) {
    const dir = deltaDirection || (typeof delta === "string" && delta.trim().startsWith("-") ? "down" : delta ? "up" : null);
    const good = dir === "up";

    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "var(--space-2)",
          padding: "var(--space-4) var(--space-5) var(--space-5)",
          background: "var(--surface-raised)",
          border: "1px solid var(--border-subtle)",
          borderRadius: "var(--radius-lg)",
          boxShadow: "var(--shadow-sm)",
          minWidth: 0,
          ...style,
        }}
        {...rest}
      >
        <span style={{ fontSize: "var(--eyebrow-size)", letterSpacing: "var(--eyebrow-tracking)", fontWeight: "var(--eyebrow-weight)", textTransform: "uppercase", color: "var(--text-tertiary)" }}>
          {label}
        </span>
        <div style={{ display: "flex", alignItems: "baseline", gap: "var(--space-3)", flexWrap: "wrap" }}>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "var(--num-lg-size)", lineHeight: "var(--num-lg-line)", letterSpacing: "var(--num-lg-tracking)", fontWeight: 500, color: "var(--text-primary)", fontVariantNumeric: "tabular-nums" }}>
            {value}
          </span>
          {delta ? (
            <span style={{ display: "inline-flex", alignItems: "center", gap: 3, fontSize: "var(--body-sm-size)", fontWeight: "var(--weight-medium)", color: good ? "var(--thyme-600)" : "var(--clay-600)" }}>
              <Icon name={good ? "arrow-up-right" : "arrow-down-right"} size={13} strokeWidth={2.25} />
              {delta}
            </span>
          ) : null}
        </div>
        {sparkline ? <div style={{ marginTop: 2 }}>{sparkline}</div> : null}
        {caption ? <span style={{ fontSize: "var(--caption-size)", color: "var(--text-tertiary)" }}>{caption}</span> : null}
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { StatTile });
})();

// ── DataTable ─────────────────────────────────────
;(function () {
  const { Icon, Checkbox } = window.Bodega;
  const DENSITY = { compact: "var(--row-compact)", default: "var(--row-default)", relaxed: "var(--row-relaxed)" };

  function DataTable({
    columns = [], rows = [], getRowId = (r, i) => r.id != null ? r.id : i,
    selectable = false, selectedIds = [], onSelectionChange,
    sortKey, sortDirection = "asc", onSortChange,
    onRowClick, density = "default", emptyState, style, ...rest
  }) {
    const [hoverRow, setHoverRow] = React.useState(null);
    const ids = rows.map(getRowId);
    const allOn = ids.length > 0 && ids.every((id) => selectedIds.includes(id));
    const someOn = !allOn && ids.some((id) => selectedIds.includes(id));
    const rowH = DENSITY[density] || DENSITY.default;

    const toggleAll = () => onSelectionChange && onSelectionChange(allOn ? [] : ids);
    const toggleOne = (id) =>
      onSelectionChange && onSelectionChange(selectedIds.includes(id) ? selectedIds.filter((x) => x !== id) : [...selectedIds, id]);

    const headCell = {
      padding: "0 var(--space-4)",
      height: 36,
      background: "var(--paper-050)",
      borderBottom: "1px solid var(--border-default)",
      fontSize: "var(--eyebrow-size)",
      letterSpacing: "var(--eyebrow-tracking)",
      fontWeight: "var(--eyebrow-weight)",
      textTransform: "uppercase",
      color: "var(--text-tertiary)",
      whiteSpace: "nowrap",
      position: "sticky",
      top: 0,
      zIndex: "var(--z-sticky)",
    };

    if (!rows.length && emptyState) {
      return <div style={{ background: "var(--surface-raised)", ...style }}>{emptyState}</div>;
    }

    return (
      <div style={{ width: "100%", overflowX: "auto", ...style }} {...rest}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "var(--body-size)" }}>
          <thead>
            <tr>
              {selectable ? (
                <th style={{ ...headCell, width: 40, padding: "0 0 0 var(--space-4)" }}>
                  <Checkbox checked={allOn} indeterminate={someOn} onChange={toggleAll} />
                </th>
              ) : null}
              {columns.map((col) => {
                const sortable = !!col.sortable && !!onSortChange;
                const active = sortKey === col.key;
                return (
                  <th
                    key={col.key}
                    style={{ ...headCell, textAlign: col.align || "left", width: col.width, cursor: sortable ? "pointer" : "default" }}
                    onClick={sortable ? () => onSortChange(col.key, active && sortDirection === "asc" ? "desc" : "asc") : undefined}
                  >
                    <span style={{ display: "inline-flex", alignItems: "center", gap: 4, color: active ? "var(--text-secondary)" : "inherit", justifyContent: col.align === "right" ? "flex-end" : "flex-start", width: "100%" }}>
                      {col.header}
                      {sortable ? (
                        <Icon name={active && sortDirection === "desc" ? "chevron-down" : "chevron-up"} size={12} strokeWidth={2.25} style={{ opacity: active ? 1 : 0.3 }} />
                      ) : null}
                    </span>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const id = getRowId(row, i);
              const on = selectedIds.includes(id);
              return (
                <tr
                  key={id}
                  onMouseEnter={() => setHoverRow(id)}
                  onMouseLeave={() => setHoverRow(null)}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                  style={{
                    height: rowH,
                    background: on ? "var(--surface-selected)" : hoverRow === id ? "var(--paper-050)" : "var(--surface-raised)",
                    cursor: onRowClick ? "pointer" : "default",
                    transition: "background-color var(--dur-instant) var(--ease-standard)",
                  }}
                >
                  {selectable ? (
                    <td style={{ padding: "0 0 0 var(--space-4)", borderBottom: "1px solid var(--border-subtle)" }} onClick={(e) => e.stopPropagation()}>
                      <Checkbox checked={on} onChange={() => toggleOne(id)} />
                    </td>
                  ) : null}
                  {columns.map((col) => (
                    <td
                      key={col.key}
                      style={{
                        padding: "0 var(--space-4)",
                        borderBottom: "1px solid var(--border-subtle)",
                        textAlign: col.align || "left",
                        color: col.muted ? "var(--text-tertiary)" : "var(--text-primary)",
                        fontFamily: col.numeric ? "var(--font-mono)" : "var(--font-sans)",
                        fontSize: col.numeric ? "var(--num-size)" : "var(--body-size)",
                        fontVariantNumeric: col.numeric ? "tabular-nums" : undefined,
                        whiteSpace: col.wrap ? "normal" : "nowrap",
                      }}
                    >
                      {col.render ? col.render(row, i) : row[col.key]}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { DataTable });
})();

// ── Tabs ──────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function Tabs({ items = [], value, onChange, variant = "underline", style, ...rest }) {
    const [hover, setHover] = React.useState(null);

    if (variant === "segmented") {
      return (
        <div
          role="tablist"
          style={{ display: "inline-flex", gap: 2, padding: 2, background: "var(--surface-sunken)", border: "1px solid var(--border-subtle)", borderRadius: "var(--radius-md)", ...style }}
          {...rest}
        >
          {items.map((it) => {
            const on = it.value === value;
            return (
              <button
                key={it.value}
                role="tab"
                aria-selected={on}
                onClick={() => onChange && onChange(it.value)}
                onMouseEnter={() => setHover(it.value)}
                onMouseLeave={() => setHover(null)}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  height: 26,
                  padding: "0 10px",
                  border: "none",
                  borderRadius: "var(--radius-sm)",
                  background: on ? "var(--surface-raised)" : hover === it.value ? "var(--surface-hover)" : "transparent",
                  boxShadow: on ? "var(--shadow-sm)" : "none",
                  color: on ? "var(--text-primary)" : "var(--text-secondary)",
                  fontSize: "var(--body-sm-size)",
                  fontWeight: "var(--weight-medium)",
                  cursor: "pointer",
                  transition: "var(--transition-control)",
                  whiteSpace: "nowrap",
                }}
              >
                {it.icon ? <Icon name={it.icon} size={14} /> : null}
                {it.label}
              </button>
            );
          })}
        </div>
      );
    }

    return (
      <div role="tablist" style={{ display: "flex", alignItems: "stretch", gap: "var(--space-1)", borderBottom: "1px solid var(--border-default)", ...style }} {...rest}>
        {items.map((it) => {
          const on = it.value === value;
          return (
            <button
              key={it.value}
              role="tab"
              aria-selected={on}
              onClick={() => onChange && onChange(it.value)}
              onMouseEnter={() => setHover(it.value)}
              onMouseLeave={() => setHover(null)}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                height: 36,
                padding: "0 10px",
                border: "none",
                background: hover === it.value && !on ? "var(--surface-hover)" : "transparent",
                borderRadius: "var(--radius-sm) var(--radius-sm) 0 0",
                boxShadow: on ? "inset 0 -2px 0 var(--thyme-600)" : "none",
                color: on ? "var(--text-primary)" : "var(--text-secondary)",
                fontSize: "var(--body-size)",
                fontWeight: on ? "var(--weight-semibold)" : "var(--weight-medium)",
                letterSpacing: "-0.004em",
                cursor: "pointer",
                marginBottom: -1,
                transition: "var(--transition-control)",
                whiteSpace: "nowrap",
              }}
            >
              {it.icon ? <Icon name={it.icon} size={14} /> : null}
              {it.label}
              {it.count != null ? (
                <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-tertiary)", background: "var(--paper-200)", borderRadius: "var(--radius-pill)", padding: "1px 5px" }}>
                  {it.count}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Tabs });
})();

// ── SideNav ───────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function SideNav({ sections = [], value, onChange, header, footer, style, ...rest }) {
    const [hover, setHover] = React.useState(null);

    return (
      <nav
        style={{
          display: "flex",
          flexDirection: "column",
          width: "var(--sidebar-width)",
          flex: "0 0 auto",
          background: "var(--paper-100)",
          borderRight: "1px solid var(--border-subtle)",
          ...style,
        }}
        {...rest}
      >
        {header ? <div style={{ padding: "var(--space-4) var(--space-3) var(--space-3)" }}>{header}</div> : null}
        <div style={{ flex: 1, overflowY: "auto", padding: "0 var(--space-2) var(--space-4)", display: "flex", flexDirection: "column", gap: "var(--space-5)" }}>
          {sections.map((section, si) => (
            <div key={section.label || si} style={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {section.label ? (
                <span style={{ padding: "0 var(--space-2) var(--space-15)", fontSize: "var(--eyebrow-size)", letterSpacing: "var(--eyebrow-tracking)", fontWeight: "var(--eyebrow-weight)", textTransform: "uppercase", color: "var(--text-tertiary)" }}>
                  {section.label}
                </span>
              ) : null}
              {(section.items || []).map((it) => {
                const on = it.value === value;
                return (
                  <button
                    key={it.value}
                    onClick={() => onChange && onChange(it.value)}
                    onMouseEnter={() => setHover(it.value)}
                    onMouseLeave={() => setHover(null)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 9,
                      width: "100%",
                      height: 30,
                      padding: "0 var(--space-2)",
                      border: "none",
                      borderRadius: "var(--radius-sm)",
                      background: on ? "var(--surface-raised)" : hover === it.value ? "var(--surface-hover)" : "transparent",
                      boxShadow: on ? "var(--shadow-xs), 0 0 0 1px var(--border-subtle)" : "none",
                      color: on ? "var(--text-primary)" : "var(--text-secondary)",
                      fontSize: "var(--body-sm-size)",
                      fontWeight: on ? "var(--weight-semibold)" : "var(--weight-medium)",
                      textAlign: "left",
                      cursor: "pointer",
                      transition: "var(--transition-control)",
                    }}
                  >
                    {it.icon ? <Icon name={it.icon} size={15} color={on ? "var(--thyme-600)" : "var(--text-tertiary)"} /> : null}
                    <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{it.label}</span>
                    {it.count != null ? (
                      <span style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-tertiary)", fontVariantNumeric: "tabular-nums" }}>{it.count}</span>
                    ) : null}
                  </button>
                );
              })}
            </div>
          ))}
        </div>
        {footer ? <div style={{ padding: "var(--space-3)", borderTop: "1px solid var(--border-subtle)" }}>{footer}</div> : null}
      </nav>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { SideNav });
})();

// ── Banner ────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const TONES = {
    info: { bg: "var(--status-info-bg)", bd: "var(--status-info-border)", fg: "var(--status-info-text)", icon: "info" },
    success: { bg: "var(--status-success-bg)", bd: "var(--status-success-border)", fg: "var(--status-success-text)", icon: "circle-check" },
    warning: { bg: "var(--status-warning-bg)", bd: "var(--status-warning-border)", fg: "var(--status-warning-text)", icon: "triangle-alert" },
    danger: { bg: "var(--status-danger-bg)", bd: "var(--status-danger-border)", fg: "var(--status-danger-text)", icon: "octagon-alert" },
  };

  function Banner({ tone = "info", title, actions, onDismiss, icon, style, children, ...rest }) {
    const t = TONES[tone] || TONES.info;
    return (
      <div
        role="status"
        style={{
          display: "flex",
          alignItems: "flex-start",
          gap: "var(--space-3)",
          padding: "var(--space-3) var(--space-4)",
          background: t.bg,
          border: `1px solid ${t.bd}`,
          borderRadius: "var(--radius-md)",
          ...style,
        }}
        {...rest}
      >
        <span style={{ marginTop: 1, color: t.fg }}><Icon name={icon || t.icon} size={16} /></span>
        <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 3 }}>
          {title ? (
            <strong style={{ fontSize: "var(--body-size)", fontWeight: "var(--weight-semibold)", color: "var(--text-primary)", letterSpacing: "-0.004em" }}>{title}</strong>
          ) : null}
          {children ? (
            <div style={{ fontSize: "var(--body-sm-size)", lineHeight: "var(--body-line)", color: "var(--text-secondary)" }}>{children}</div>
          ) : null}
          {actions ? <div style={{ display: "flex", gap: "var(--space-2)", marginTop: "var(--space-15)" }}>{actions}</div> : null}
        </div>
        {onDismiss ? (
          <button type="button" aria-label="Dismiss" onClick={onDismiss}
            style={{ display: "flex", border: "none", background: "transparent", color: "var(--text-tertiary)", cursor: "pointer", padding: 2, marginTop: -1 }}>
            <Icon name="x" size={14} />
          </button>
        ) : null}
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Banner });
})();

// ── Toast ─────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const TONES = { neutral: "check", success: "circle-check", danger: "octagon-alert", warning: "triangle-alert" };

  function Toast({ tone = "neutral", message, action, onDismiss, visible = true, style, ...rest }) {
    return (
      <div
        role="status"
        aria-live="polite"
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "var(--space-3)",
          padding: "10px 12px 10px 14px",
          background: "var(--surface-inverse)",
          color: "var(--text-inverse)",
          borderRadius: "var(--radius-md)",
          boxShadow: "var(--shadow-pop)",
          fontSize: "var(--body-size)",
          maxWidth: 420,
          opacity: visible ? 1 : 0,
          transform: visible ? "translateY(0)" : "translateY(8px)",
          transition: "opacity var(--dur-base) var(--ease-standard), transform var(--dur-base) var(--ease-lift)",
          ...style,
        }}
        {...rest}
      >
        <Icon name={TONES[tone] || TONES.neutral} size={16} color={tone === "danger" ? "#f0a99e" : tone === "warning" ? "var(--ochre-200)" : "var(--thyme-300)"} />
        <span style={{ flex: 1 }}>{message}</span>
        {action ? (
          <button type="button" onClick={action.onClick}
            style={{ border: "none", background: "transparent", color: "var(--thyme-300)", fontSize: "var(--body-size)", fontWeight: "var(--weight-semibold)", cursor: "pointer", padding: "0 2px", whiteSpace: "nowrap" }}>
            {action.label}
          </button>
        ) : null}
        {onDismiss ? (
          <button type="button" aria-label="Dismiss" onClick={onDismiss}
            style={{ display: "flex", border: "none", background: "transparent", color: "var(--ink-300)", cursor: "pointer", padding: 2 }}>
            <Icon name="x" size={14} />
          </button>
        ) : null}
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Toast });
})();

// ── Tooltip ───────────────────────────────────────
;(function () {
  function Tooltip({ label, placement = "top", delay = 250, style, children, ...rest }) {
    const [open, setOpen] = React.useState(false);
    const timer = React.useRef(null);

    const show = () => { clearTimeout(timer.current); timer.current = setTimeout(() => setOpen(true), delay); };
    const hide = () => { clearTimeout(timer.current); setOpen(false); };
    React.useEffect(() => () => clearTimeout(timer.current), []);

    const pos = {
      top: { bottom: "calc(100% + 6px)", left: "50%", transform: "translateX(-50%)" },
      bottom: { top: "calc(100% + 6px)", left: "50%", transform: "translateX(-50%)" },
      left: { right: "calc(100% + 6px)", top: "50%", transform: "translateY(-50%)" },
      right: { left: "calc(100% + 6px)", top: "50%", transform: "translateY(-50%)" },
    }[placement];

    return (
      <span
        onMouseEnter={show}
        onMouseLeave={hide}
        onFocus={show}
        onBlur={hide}
        style={{ position: "relative", display: "inline-flex", ...style }}
        {...rest}
      >
        {children}
        <span
          role="tooltip"
          style={{
            position: "absolute",
            ...pos,
            zIndex: "var(--z-dropdown)",
            padding: "5px 8px",
            background: "var(--surface-inverse)",
            color: "var(--text-inverse)",
            borderRadius: "var(--radius-sm)",
            boxShadow: "var(--shadow-lg)",
            fontSize: "var(--caption-size)",
            lineHeight: 1.35,
            whiteSpace: "nowrap",
            pointerEvents: "none",
            opacity: open ? 1 : 0,
            transition: "opacity var(--dur-fast) var(--ease-standard)",
          }}
        >
          {label}
        </span>
      </span>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Tooltip });
})();

// ── EmptyState ────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  function EmptyState({ icon = "inbox", title, description, actions, size = "md", style, ...rest }) {
    const lg = size === "lg";
    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          gap: "var(--space-3)",
          textAlign: "center",
          padding: lg ? "var(--space-10) var(--space-6)" : "var(--space-8) var(--space-6)",
          ...style,
        }}
        {...rest}
      >
        <span
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: lg ? 52 : 44,
            height: lg ? 52 : 44,
            borderRadius: "var(--radius-xl)",
            background: "var(--paper-100)",
            border: "1px solid var(--border-subtle)",
            color: "var(--ink-400)",
            marginBottom: 2,
          }}
        >
          <Icon name={icon} size={lg ? 24 : 20} strokeWidth={1.5} />
        </span>
        {title ? (
          <h3 style={{ margin: 0, fontFamily: "var(--font-display)", fontSize: lg ? "var(--display-3-size)" : "var(--title-1-size)", lineHeight: 1.2, letterSpacing: "-0.014em", fontWeight: 500, color: "var(--text-primary)" }}>
            {title}
          </h3>
        ) : null}
        {description ? (
          <p style={{ margin: 0, maxWidth: 380, fontSize: "var(--body-size)", lineHeight: "var(--body-line)", color: "var(--text-tertiary)" }}>{description}</p>
        ) : null}
        {actions ? <div style={{ display: "flex", gap: "var(--space-2)", marginTop: "var(--space-2)" }}>{actions}</div> : null}
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { EmptyState });
})();

// ── Dialog ────────────────────────────────────────
;(function () {
  const { Icon } = window.Bodega;
  const WIDTHS = { sm: 400, md: 520, lg: 680 };

  function Dialog({ open = false, title, description, size = "md", onClose, footer, closeOnScrim = true, style, children, ...rest }) {
    React.useEffect(() => {
      if (!open || !onClose) return;
      const onKey = (e) => { if (e.key === "Escape") onClose(); };
      document.addEventListener("keydown", onKey);
      return () => document.removeEventListener("keydown", onKey);
    }, [open, onClose]);

    if (!open) return null;

    return (
      <div
        style={{
          position: "fixed",
          inset: 0,
          zIndex: "var(--z-overlay)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "var(--space-6)",
          background: "var(--scrim)",
          backdropFilter: "var(--blur-overlay)",
          WebkitBackdropFilter: "var(--blur-overlay)",
          animation: "bodega-fade var(--dur-fast) var(--ease-standard)",
        }}
        onClick={closeOnScrim && onClose ? (e) => { if (e.target === e.currentTarget) onClose(); } : undefined}
      >
        <div
          role="dialog"
          aria-modal="true"
          style={{
            width: "100%",
            maxWidth: WIDTHS[size] || WIDTHS.md,
            maxHeight: "calc(100vh - 96px)",
            display: "flex",
            flexDirection: "column",
            background: "var(--surface-raised)",
            borderRadius: "var(--radius-xl)",
            boxShadow: "var(--shadow-pop)",
            overflow: "hidden",
            animation: "bodega-rise var(--dur-base) var(--ease-lift)",
            ...style,
          }}
          {...rest}
        >
          <header style={{ display: "flex", alignItems: "flex-start", gap: "var(--space-4)", padding: "var(--space-5) var(--space-5) var(--space-3)" }}>
            <div style={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 4 }}>
              {title ? (
                <h2 style={{ margin: 0, fontFamily: "var(--font-sans)", fontSize: "var(--title-2-size)", lineHeight: "var(--title-2-line)", letterSpacing: "var(--title-2-tracking)", fontWeight: "var(--weight-semibold)", color: "var(--text-primary)" }}>
                  {title}
                </h2>
              ) : null}
              {description ? (
                <p style={{ margin: 0, fontSize: "var(--body-size)", lineHeight: "var(--body-line)", color: "var(--text-tertiary)" }}>{description}</p>
              ) : null}
            </div>
            {onClose ? (
              <button type="button" aria-label="Close" onClick={onClose}
                style={{ display: "flex", border: "none", background: "transparent", color: "var(--text-tertiary)", cursor: "pointer", padding: 3, marginTop: -2, borderRadius: "var(--radius-sm)" }}>
                <Icon name="x" size={16} />
              </button>
            ) : null}
          </header>
          {children ? <div style={{ padding: "0 var(--space-5) var(--space-5)", overflowY: "auto" }}>{children}</div> : null}
          {footer ? (
            <footer style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: "var(--space-2)", padding: "var(--space-3) var(--space-5)", borderTop: "1px solid var(--border-subtle)", background: "var(--paper-050)" }}>
              {footer}
            </footer>
          ) : null}
        </div>
      </div>
    );
  }

  window.Bodega = Object.assign(window.Bodega || {}, { Dialog });
})();
