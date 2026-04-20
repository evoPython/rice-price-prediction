"""
evaluate.py
-----------
Full model evaluation:
  1. Compute MAE / RMSE / MAPE / ME for every model
  2. Run pairwise Diebold-Mariano tests
  3. Run Model Confidence Set (MCS)
  4. SHAP feature contribution analysis
  5. Save results tables and plots to results/
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

from src.evaluation.metrics          import evaluate_all
from src.evaluation.statistical_tests import run_pairwise_dm, model_confidence_set
from src.utils.helpers               import get_logger, save_results

logger = get_logger(__name__)

RESULTS_DIR = Path(__file__).resolve().parents[2] / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "figure.dpi":  150,
    "font.size":   10,
    "axes.spines.top":   False,
    "axes.spines.right": False,
})


# ---------------------------------------------------------------------------
# 1. Per-model metrics
# ---------------------------------------------------------------------------

def evaluate_models(predictions: dict) -> pd.DataFrame:
    """
    Computes all four metrics for each model.

    Parameters
    ----------
    predictions : {model_name: {'y_pred': array, 'y_test': array}}

    Returns
    -------
    DataFrame with one row per model and columns MAE, RMSE, MAPE, ME
    """
    rows = []
    for name, data in predictions.items():
        metrics = evaluate_all(data["y_test"], data["y_pred"])
        metrics["Model"] = name.upper()
        rows.append(metrics)

    df = pd.DataFrame(rows).set_index("Model")[["MAE", "RMSE", "MAPE", "ME"]]
    df = df.sort_values("RMSE")

    logger.info("\n=== Model Metrics ===\n" + df.round(4).to_string())

    # Save CSV
    df.to_csv(RESULTS_DIR / "metrics.csv")
    logger.info(f"Metrics saved to {RESULTS_DIR / 'metrics.csv'}")

    return df


# ---------------------------------------------------------------------------
# 2. Statistical tests
# ---------------------------------------------------------------------------

def run_statistical_tests(predictions: dict, loss: str = "mse") -> dict:
    """
    Runs DM pairwise tests and MCS.

    Parameters
    ----------
    predictions : {model_name: {'y_pred': array, 'y_test': array}}
    loss        : 'mse' or 'mae'

    Returns
    -------
    dict with 'dm_results' (DataFrame) and 'mcs_results' (dict)
    """
    # Extract errors (y_true − y_pred) and raw predictions per model
    errors    = {}
    preds_raw = {}
    y_true_ref = None

    for name, data in predictions.items():
        y_test = data["y_test"]
        y_pred = data["y_pred"]
        errors[name]    = y_test - y_pred
        preds_raw[name] = y_pred
        if y_true_ref is None:
            y_true_ref = y_test

    # DM pairwise
    dm_df = run_pairwise_dm(errors, loss=loss)
    dm_df.to_csv(RESULTS_DIR / "dm_tests.csv", index=False)
    logger.info(f"DM test results saved to {RESULTS_DIR / 'dm_tests.csv'}")
    logger.info("\n=== Diebold-Mariano Tests ===\n" + dm_df.to_string(index=False))

    # MCS
    mcs_result = model_confidence_set(preds_raw, y_true_ref, loss=loss)
    logger.info(f"MCS set: {mcs_result['mcs_set']}")

    return {
        "dm_results":  dm_df,
        "mcs_results": mcs_result,
    }


# ---------------------------------------------------------------------------
# 3. SHAP Feature Contribution Analysis
# ---------------------------------------------------------------------------

def shap_analysis(
    models: dict,
    preprocessed: dict,
    feature_names: list,
    max_display: int = 15,
):
    """
    Computes SHAP values for each model and saves summary plots.

    Tree-based models (RF, XGBoost, HADT AdaBoost) → TreeExplainer (fast).
    LSTM → KernelSHAP on a background sample.

    Parameters
    ----------
    models       : {model_name: {'model': fitted_model, ...}} from pipeline
    preprocessed : output of preprocess()
    feature_names: list of feature column names
    max_display  : max features to show in plots

    Returns
    -------
    dict {model_name: mean_abs_shap_values (pd.Series)}
    """
    try:
        import shap
    except ImportError:
        logger.warning("SHAP not installed. Skipping analysis. "
                       "Run: pip install shap")
        return {}

    shap_dir = RESULTS_DIR / "shap"
    shap_dir.mkdir(parents=True, exist_ok=True)

    X_val = preprocessed["X_train_raw"]   # use training set for SHAP background
    X_val_np = preprocessed["X_train"]

    shap_importances = {}

    for name, info in models.items():
        model = info["model"]
        logger.info(f"Computing SHAP values: {name.upper()} …")

        try:
            if name in ("rf", "xgb", "hadt"):
                # TreeExplainer for tree-based models
                explainer = shap.TreeExplainer(
                    model if name != "hadt" else model.model_
                )
                shap_values = explainer.shap_values(X_val_np)

            elif name == "lstm":
                # KernelSHAP for LSTM (slower – use a background summary)
                background = shap.kmeans(X_val_np, 10)
                X_lstm_test = preprocessed["X_lstm_test"]

                def lstm_wrapper(X):
                    return info["predict_fn"](X)

                explainer = shap.KernelExplainer(lstm_wrapper, background)
                # Use a small sample to keep runtime manageable
                n_samples = min(50, len(X_lstm_test))
                shap_values = explainer.shap_values(
                    X_lstm_test[:n_samples], nsamples=100
                )
            else:
                logger.warning(f"No SHAP strategy defined for {name}. Skipping.")
                continue

            # Mean |SHAP| across observations
            mean_abs = pd.Series(
                np.abs(shap_values).mean(axis=0),
                index=feature_names,
            ).sort_values(ascending=False)
            shap_importances[name] = mean_abs

            # Bar chart
            fig, ax = plt.subplots(figsize=(8, 5))
            top = mean_abs.head(max_display)
            ax.barh(top.index[::-1], top.values[::-1], color="steelblue")
            ax.set_xlabel("Mean |SHAP value|")
            ax.set_title(f"SHAP Feature Importance – {name.upper()}")
            plt.tight_layout()
            fig.savefig(shap_dir / f"shap_{name}.png")
            plt.close(fig)
            logger.info(f"  SHAP plot saved: shap_{name}.png")

        except Exception as e:
            logger.warning(f"SHAP analysis failed for {name}: {e}")

    # Combined ranked importance table
    if shap_importances:
        combined = pd.DataFrame(shap_importances).fillna(0)
        combined.to_csv(shap_dir / "shap_importances.csv")
        logger.info(f"Combined SHAP importances saved to {shap_dir}")

    return shap_importances


# ---------------------------------------------------------------------------
# 4. Result Visualisations
# ---------------------------------------------------------------------------

def plot_predictions(predictions: dict, save: bool = True):
    """
    Actual vs Predicted scatter + time-series line plots for each model.
    """
    plots_dir = RESULTS_DIR / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    n_models = len(predictions)
    fig, axes = plt.subplots(1, n_models, figsize=(5 * n_models, 4))
    if n_models == 1:
        axes = [axes]

    for ax, (name, data) in zip(axes, predictions.items()):
        y_test = data["y_test"]
        y_pred = data["y_pred"]

        ax.scatter(y_test, y_pred, alpha=0.6, s=25, label="Predictions", color="steelblue")
        lo, hi = min(y_test.min(), y_pred.min()), max(y_test.max(), y_pred.max())
        ax.plot([lo, hi], [lo, hi], "r--", lw=1.2, label="Perfect fit")
        ax.set_xlabel("Actual Price (₱)")
        ax.set_ylabel("Predicted Price (₱)")
        ax.set_title(f"{name.upper()}")
        ax.legend(fontsize=8)

    fig.suptitle("Actual vs. Predicted Rice Prices", fontsize=12, fontweight="bold")
    plt.tight_layout()

    if save:
        fig.savefig(plots_dir / "actual_vs_predicted.png")
        logger.info("Saved: actual_vs_predicted.png")
    plt.close(fig)


def plot_time_series(predictions: dict, save: bool = True):
    """
    Time-series plot of actual prices vs each model's predictions.
    """
    plots_dir = RESULTS_DIR / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Use the test set from the first model as the actual reference
    first = next(iter(predictions.values()))
    y_true = first["y_test"]
    n_test = len(y_true)
    x_axis = np.arange(n_test)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x_axis, y_true, "k-", lw=2, label="Actual", zorder=5)

    colors = ["steelblue", "darkorange", "green", "red"]
    for (name, data), color in zip(predictions.items(), colors):
        ax.plot(x_axis, data["y_pred"], "--", lw=1.4,
                color=color, label=name.upper(), alpha=0.85)

    ax.set_xlabel("Test Sample Index")
    ax.set_ylabel("Rice Price (₱/kg)")
    ax.set_title("Rice Price Forecasts vs Actual (Test Set)")
    ax.legend()
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("₱%.2f"))
    plt.tight_layout()

    if save:
        fig.savefig(plots_dir / "time_series_forecast.png")
        logger.info("Saved: time_series_forecast.png")
    plt.close(fig)


def plot_metrics_comparison(metrics_df: pd.DataFrame, save: bool = True):
    """
    Side-by-side bar chart of MAE, RMSE, MAPE, ME for all models.
    """
    plots_dir = RESULTS_DIR / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    metric_cols = ["MAE", "RMSE", "MAPE", "ME"]
    fig, axes = plt.subplots(1, len(metric_cols), figsize=(14, 4))

    for ax, metric in zip(axes, metric_cols):
        vals = metrics_df[metric]
        colors = ["steelblue" if v == vals.min() else "lightgray" for v in vals]
        bars = ax.bar(vals.index, vals.values, color=colors, edgecolor="white")
        ax.set_title(metric, fontweight="bold")
        ax.set_xlabel("")
        ax.tick_params(axis="x", rotation=30)
        # Annotate bars
        for bar, val in zip(bars, vals.values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.02,
                f"{val:.3f}",
                ha="center", va="bottom", fontsize=8,
            )

    fig.suptitle("Model Performance Comparison", fontsize=12, fontweight="bold")
    plt.tight_layout()

    if save:
        fig.savefig(plots_dir / "metrics_comparison.png")
        logger.info("Saved: metrics_comparison.png")
    plt.close(fig)


# ---------------------------------------------------------------------------
# 5. Master evaluation runner
# ---------------------------------------------------------------------------

def full_evaluation(
    predictions: dict,
    models: dict,
    preprocessed: dict,
    feature_names: list,
    run_shap: bool = True,
) -> dict:
    """
    Runs the complete evaluation pipeline and saves all artefacts.

    Parameters
    ----------
    predictions   : output of pipeline.get_all_predictions()
    models        : output of pipeline.train_all_models()
    preprocessed  : output of preprocess.preprocess()
    feature_names : list of feature column names
    run_shap      : whether to compute SHAP values (can be slow for LSTM)

    Returns
    -------
    dict with 'metrics', 'dm_results', 'mcs_results', 'shap_importances'
    """
    logger.info("=" * 60)
    logger.info("FULL MODEL EVALUATION")
    logger.info("=" * 60)

    # Metrics
    metrics_df = evaluate_models(predictions)

    # Statistical tests
    stat_results = run_statistical_tests(predictions)

    # Plots
    plot_predictions(predictions)
    plot_time_series(predictions)
    plot_metrics_comparison(metrics_df)

    # SHAP
    shap_importances = {}
    if run_shap:
        shap_importances = shap_analysis(models, preprocessed, feature_names)

    # Persist all results as JSON
    all_results = {
        "metrics":     metrics_df.round(6).to_dict(),
        "mcs_set":     stat_results["mcs_results"]["mcs_set"],
        "eliminated":  stat_results["mcs_results"]["eliminated"],
        "mcs_p_values":stat_results["mcs_results"]["p_values"],
    }
    save_results(all_results, "evaluation_results.json")

    logger.info("=" * 60)
    logger.info(f"Best model (lowest RMSE): {metrics_df['RMSE'].idxmin()}")
    logger.info(f"MCS (top models): {stat_results['mcs_results']['mcs_set']}")
    logger.info("All results saved to results/")
    logger.info("=" * 60)

    return {
        "metrics":          metrics_df,
        "dm_results":       stat_results["dm_results"],
        "mcs_results":      stat_results["mcs_results"],
        "shap_importances": shap_importances,
    }
