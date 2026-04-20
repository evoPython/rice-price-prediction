"""
report.py
---------
Final pipeline step: generate a visual report and an AI-written narrative
summary of the model evaluation results.

Two public functions
--------------------
  generate_report_figures(predictions, metrics_df, dm_df, mcs_result)
      Produces a multi-panel PNG dashboard saved to results/report/.

  generate_narrative(metrics_df, dm_df, mcs_result, target_col)
      Calls the Anthropic API (claude-sonnet-4-20250514) and returns a
      plain-English paragraph-style explanation of what the numbers mean.

  run_report(predictions, metrics_df, dm_df, mcs_result, target_col)
      Convenience wrapper that runs both steps and logs everything.
"""

import json
import textwrap
import urllib.request
import urllib.error
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker

from src.utils.helpers import get_logger

logger = get_logger(__name__)

RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"
REPORT_DIR  = RESULTS_DIR / "report"

plt.rcParams.update({
    "figure.dpi":        150,
    "font.size":         9,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.titlesize":    10,
    "axes.titleweight":  "bold",
})

_PALETTE = {
    "hadt": "#2563EB",   # blue
    "xgb":  "#16A34A",   # green
    "rf":   "#D97706",   # amber
    "lstm": "#DC2626",   # red
    "actual": "#111827", # near-black
}
_LABEL = {"hadt": "HADT", "xgb": "XGBoost", "rf": "Random Forest", "lstm": "LSTM"}


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Visualisations
# ─────────────────────────────────────────────────────────────────────────────

def generate_report_figures(
    predictions: dict,
    metrics_df:  pd.DataFrame,
    dm_df:       pd.DataFrame,
    mcs_result:  dict,
    target_col:  str = "price_well_milled",
    save:        bool = True,
) -> Path:
    """
    Builds a six-panel dashboard PNG:
      [A] Time-series forecast vs actual
      [B] Actual vs Predicted scatter (best model only)
      [C] Residuals over time for every model
      [D] MAE / RMSE / MAPE bar comparison
      [E] ME (bias) bar chart with zero-line
      [F] Diebold-Mariano significance heatmap

    Returns the path to the saved PNG.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    model_order = list(predictions.keys())
    first       = predictions[model_order[0]]
    y_true      = first["y_test"]
    n           = len(y_true)
    x_idx       = np.arange(n)

    # ── Layout ───────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 12))
    gs  = gridspec.GridSpec(
        3, 3,
        figure=fig,
        hspace=0.52,
        wspace=0.38,
        left=0.06, right=0.97,
        top=0.93,  bottom=0.07,
    )

    ax_ts      = fig.add_subplot(gs[0, :])          # A – full-width top row
    ax_scatter = fig.add_subplot(gs[1, 0])           # B
    ax_resid   = fig.add_subplot(gs[1, 1:])          # C – two-thirds width
    ax_bar     = fig.add_subplot(gs[2, 0])           # D
    ax_bias    = fig.add_subplot(gs[2, 1])           # E
    ax_dm      = fig.add_subplot(gs[2, 2])           # F

    # ── A: Time-series ───────────────────────────────────────────────────────
    ax_ts.plot(x_idx, y_true, color=_PALETTE["actual"],
               lw=2.2, zorder=6, label="Actual")
    for name, data in predictions.items():
        ax_ts.plot(x_idx, data["y_pred"],
                   color=_PALETTE.get(name, "gray"),
                   lw=1.4, ls="--", alpha=0.85, label=_LABEL.get(name, name.upper()))
    ax_ts.set_title("A  |  Rice Price Forecasts vs Actual (Test Set)")
    ax_ts.set_xlabel("Test Sample Index")
    ax_ts.set_ylabel(f"Price (₱/kg)")
    ax_ts.yaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.2f"))
    ax_ts.legend(ncol=len(predictions) + 1, fontsize=8, loc="upper left")

    # ── B: Actual vs Predicted scatter (best RMSE model) ────────────────────
    best_name = metrics_df["RMSE"].idxmin().lower()
    best_pred = predictions.get(best_name, predictions[model_order[0]])
    y_pred_b  = best_pred["y_pred"]
    lo = min(y_true.min(), y_pred_b.min())
    hi = max(y_true.max(), y_pred_b.max())
    ax_scatter.scatter(y_true, y_pred_b,
                       color=_PALETTE.get(best_name, "steelblue"),
                       alpha=0.75, s=35, zorder=3)
    ax_scatter.plot([lo, hi], [lo, hi], "k--", lw=1.1, alpha=0.5, label="Perfect fit")
    ax_scatter.set_title(f"B  |  Actual vs Predicted\n({_LABEL.get(best_name, best_name.upper())} – best model)")
    ax_scatter.set_xlabel("Actual (₱/kg)")
    ax_scatter.set_ylabel("Predicted (₱/kg)")
    ax_scatter.xaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.0f"))
    ax_scatter.yaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.0f"))

    # ── C: Residuals over time (all models) ─────────────────────────────────
    ax_resid.axhline(0, color="black", lw=0.9, ls="-", alpha=0.4)
    for name, data in predictions.items():
        resid = data["y_test"] - data["y_pred"]
        ax_resid.plot(x_idx, resid,
                      color=_PALETTE.get(name, "gray"),
                      lw=1.3, alpha=0.8, label=_LABEL.get(name, name.upper()))
    ax_resid.set_title("C  |  Residuals over Time  (actual − predicted)")
    ax_resid.set_xlabel("Test Sample Index")
    ax_resid.set_ylabel("Residual (₱/kg)")
    ax_resid.yaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.1f"))
    ax_resid.legend(fontsize=8)

    # ── D: MAE / RMSE / MAPE grouped bars ───────────────────────────────────
    metric_cols   = ["MAE", "RMSE", "MAPE"]
    n_metrics     = len(metric_cols)
    n_models      = len(metrics_df)
    bar_w         = 0.22
    x_pos         = np.arange(n_metrics)
    model_indices = list(metrics_df.index)

    for i, mname in enumerate(model_indices):
        vals   = metrics_df.loc[mname, metric_cols].values
        offset = (i - n_models / 2 + 0.5) * bar_w
        color  = _PALETTE.get(mname.lower(), "gray")
        ax_bar.bar(x_pos + offset, vals, bar_w,
                   color=color, alpha=0.85, label=mname, zorder=3)
    ax_bar.set_title("D  |  Error Metrics by Model")
    ax_bar.set_xticks(x_pos)
    ax_bar.set_xticklabels(metric_cols)
    ax_bar.set_ylabel("Error (₱ or %)")
    ax_bar.legend(fontsize=7)
    ax_bar.grid(axis="y", alpha=0.25)

    # ── E: ME (bias) bar chart ───────────────────────────────────────────────
    me_vals  = metrics_df["ME"]
    colors_e = [
        "#2563EB" if v >= 0 else "#DC2626"
        for v in me_vals.values
    ]
    bars_e = ax_bias.bar(me_vals.index, me_vals.values, color=colors_e,
                         alpha=0.85, zorder=3)
    ax_bias.axhline(0, color="black", lw=0.9, alpha=0.5)
    for bar, val in zip(bars_e, me_vals.values):
        ax_bias.text(
            bar.get_x() + bar.get_width() / 2,
            val + (0.05 if val >= 0 else -0.12),
            f"₱{val:+.2f}",
            ha="center", va="bottom" if val >= 0 else "top",
            fontsize=7.5,
        )
    ax_bias.set_title("E  |  Mean Error (Bias)\n+ = under-predict  |  − = over-predict")
    ax_bias.set_ylabel("Mean Error (₱/kg)")
    ax_bias.tick_params(axis="x", rotation=20)

    # ── F: DM test heatmap ───────────────────────────────────────────────────
    model_names_dm = sorted(set(dm_df["model_1"].tolist() + dm_df["model_2"].tolist()))
    n_dm = len(model_names_dm)
    sig_matrix = np.full((n_dm, n_dm), np.nan)
    pval_matrix = np.full((n_dm, n_dm), np.nan)

    idx_map = {m: i for i, m in enumerate(model_names_dm)}
    for _, row in dm_df.iterrows():
        i, j = idx_map[row["model_1"]], idx_map[row["model_2"]]
        pval_matrix[i, j] = row["p_value"]
        pval_matrix[j, i] = row["p_value"]
        sig_matrix[i, j]  = 1.0 if row["significant_at_5pct"] else 0.0
        sig_matrix[j, i]  = sig_matrix[i, j]

    im = ax_dm.imshow(pval_matrix, cmap="RdYlGn_r", vmin=0, vmax=0.2, aspect="auto")
    ax_dm.set_xticks(range(n_dm))
    ax_dm.set_yticks(range(n_dm))
    ax_dm.set_xticklabels([m.upper() for m in model_names_dm], fontsize=8, rotation=30)
    ax_dm.set_yticklabels([m.upper() for m in model_names_dm], fontsize=8)
    for i in range(n_dm):
        for j in range(n_dm):
            if not np.isnan(pval_matrix[i, j]):
                marker = "✱" if sig_matrix[i, j] == 1.0 else ""
                ax_dm.text(j, i, f"{pval_matrix[i,j]:.3f}{marker}",
                           ha="center", va="center", fontsize=7.5,
                           color="white" if pval_matrix[i, j] < 0.08 else "black")
    plt.colorbar(im, ax=ax_dm, fraction=0.046, pad=0.04).set_label("p-value", fontsize=7)
    ax_dm.set_title("F  |  DM Test p-values\n✱ = significant at 5%")

    # ── Suptitle & save ──────────────────────────────────────────────────────
    target_label = target_col.replace("_", " ").title()
    fig.suptitle(
        f"Rice Price Prediction — Model Evaluation Dashboard ({target_label})",
        fontsize=13, fontweight="bold", y=0.97,
    )

    out_path = REPORT_DIR / "evaluation_dashboard.png"
    if save:
        fig.savefig(out_path, bbox_inches="tight")
        logger.info(f"Dashboard saved: {out_path}")
    plt.close(fig)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# 2.  AI narrative via Anthropic API
# ─────────────────────────────────────────────────────────────────────────────

def _build_prompt(
    metrics_df: pd.DataFrame,
    dm_df:      pd.DataFrame,
    mcs_result: dict,
    target_col: str,
) -> str:
    """Assembles the data context string fed to the LLM."""
    target_label = target_col.replace("_", " ")

    metrics_str = metrics_df.round(4).to_string()
    dm_str      = dm_df.to_string(index=False)
    mcs_set     = mcs_result.get("mcs_set", [])
    eliminated  = mcs_result.get("eliminated", [])

    return textwrap.dedent(f"""
    You are a research assistant summarising machine-learning model evaluation
    results for a study on {target_label} forecasting in Cebu, Philippines.

    Write a concise but informative explanation (3–5 short paragraphs) of the
    results below, in plain English suitable for including in a research report.
    Use natural academic-ish language — not bullet points. Do not repeat every
    single number; focus on the story the results tell.

    ── Evaluation Metrics ──
    {metrics_str}

    (MAE and RMSE are in ₱/kg. MAPE is in %. ME is the mean bias: positive =
    model under-predicts, negative = model over-predicts.)

    ── Pairwise Diebold-Mariano Tests ──
    {dm_str}

    ── Model Confidence Set (MCS, α=0.10) ──
    MCS (statistically equivalent best models): {mcs_set}
    Eliminated models: {eliminated}

    Guidelines:
    - Open with which model performed best overall and by how much.
    - Discuss what the ME values reveal about bias direction for each model.
    - Summarise what the DM tests and MCS tell us about statistical significance.
    - Note any model that stood out negatively and suggest a likely reason.
    - Close with a practical takeaway for stakeholders (farmers, policymakers).
    - Keep total length under 250 words.
    """).strip()


def generate_narrative(
    metrics_df: pd.DataFrame,
    dm_df:      pd.DataFrame,
    mcs_result: dict,
    target_col: str = "price_well_milled",
    save:       bool = True,
) -> str:
    """
    Calls the Anthropic Messages API and returns a plain-English explanation
    of the evaluation results.

    Requires the ANTHROPIC_API_KEY env variable, OR relies on the proxy
    injected by the Claude.ai runtime (no explicit key needed there).

    Returns the generated text string. On any API failure the function
    falls back to a structured plain-text summary so the pipeline never
    crashes.
    """
    prompt = _build_prompt(metrics_df, dm_df, mcs_result, target_col)

    payload = json.dumps({
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages":   [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        method="POST",
        headers={
            "Content-Type":      "application/json",
            "anthropic-version": "2023-06-01",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body     = json.loads(resp.read().decode("utf-8"))
            narrative = body["content"][0]["text"].strip()
            logger.info("Narrative generated successfully via Anthropic API.")
    except Exception as exc:
        logger.warning(f"Anthropic API call failed ({exc}). Falling back to auto-summary.")
        narrative = _fallback_narrative(metrics_df, dm_df, mcs_result, target_col)

    if save:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = REPORT_DIR / "narrative_summary.txt"
        out_path.write_text(narrative, encoding="utf-8")
        logger.info(f"Narrative saved: {out_path}")

    return narrative


def _fallback_narrative(
    metrics_df: pd.DataFrame,
    dm_df:      pd.DataFrame,
    mcs_result: dict,
    target_col: str,
) -> str:
    """
    Generates a structured plain-text summary without the API, used as a
    fallback if the Anthropic call fails.
    """
    best    = metrics_df["RMSE"].idxmin()
    worst   = metrics_df["RMSE"].idxmax()
    mcs_set = mcs_result.get("mcs_set", [])
    target_label = target_col.replace("_", " ")

    sig_pairs = dm_df[dm_df["significant_at_5pct"] == True]
    n_sig     = len(sig_pairs)

    lines = [
        f"Evaluation Summary — {target_label}",
        "=" * 50,
        "",
        f"Best model (lowest RMSE): {best}",
        f"  MAE  = ₱{metrics_df.loc[best, 'MAE']:.4f}/kg",
        f"  RMSE = ₱{metrics_df.loc[best, 'RMSE']:.4f}/kg",
        f"  MAPE = {metrics_df.loc[best, 'MAPE']:.2f}%",
        f"  ME   = ₱{metrics_df.loc[best, 'ME']:+.4f}/kg",
        "",
        f"Worst model (highest RMSE): {worst}",
        f"  RMSE = ₱{metrics_df.loc[worst, 'RMSE']:.4f}/kg",
        f"  MAPE = {metrics_df.loc[worst, 'MAPE']:.2f}%",
        "",
        f"Model Confidence Set (α=0.10): {mcs_set}",
        f"DM tests significant at 5%: {n_sig} of {len(dm_df)} pairs",
        "",
        "Bias (ME) by model:",
    ]
    for mname in metrics_df.index:
        me_val = metrics_df.loc[mname, "ME"]
        direction = "under-predicts" if me_val > 0 else "over-predicts"
        lines.append(f"  {mname}: ₱{me_val:+.4f}/kg ({direction})")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Master runner
# ─────────────────────────────────────────────────────────────────────────────

def run_report(
    predictions: dict,
    metrics_df:  pd.DataFrame,
    dm_df:       pd.DataFrame,
    mcs_result:  dict,
    target_col:  str = "price_well_milled",
) -> dict:
    """
    Convenience wrapper that:
      1. Generates the six-panel dashboard PNG.
      2. Generates and saves the AI narrative summary.
      3. Logs and returns both outputs.

    Parameters
    ----------
    predictions : {model_name: {'y_pred': array, 'y_test': array}}
    metrics_df  : DataFrame from evaluate_models()
    dm_df       : DataFrame from run_pairwise_dm()
    mcs_result  : dict from model_confidence_set()
    target_col  : name of the target column (for labelling)

    Returns
    -------
    dict with keys 'dashboard_path' (Path) and 'narrative' (str)
    """
    logger.info("━" * 55)
    logger.info("STEP 5: Generating report")
    logger.info("━" * 55)

    dashboard_path = generate_report_figures(
        predictions, metrics_df, dm_df, mcs_result, target_col
    )

    narrative = generate_narrative(
        metrics_df, dm_df, mcs_result, target_col
    )

    logger.info("─" * 55)
    logger.info("NARRATIVE SUMMARY")
    logger.info("─" * 55)
    for line in narrative.split("\n"):
        logger.info(line)
    logger.info("─" * 55)
    logger.info(f"Report saved to: {REPORT_DIR}")

    return {
        "dashboard_path": dashboard_path,
        "narrative":      narrative,
    }
