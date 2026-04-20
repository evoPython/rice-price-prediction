"""
main.py
-------
Interactive entry point for the rice price prediction research pipeline.

The script can run as a terminal UI (default) or as a one-shot CLI.

Usage
-----
# Interactive terminal UI
python main.py

# Run a full pipeline non-interactively
python main.py --mode full

# Skip hyperparameter tuning
python main.py --mode full --no-tune

# Generate EDA plots only
python main.py --mode eda

# Re-run evaluation on already-trained models
python main.py --mode eval-only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd

from src.analysis.eda import run_eda
from src.data.merge_data import merge_datasets
from src.data.preprocess import (
    get_preprocessed_bundle_path,
    get_target_artifact_dir,
    load_cached_preprocessed,
    preprocess,
)
from src.models.pipeline import train_all_models, get_all_predictions
from src.evaluation.evaluate import full_evaluation
from src.evaluation.report import run_report
from src.utils.helpers import get_logger, set_seeds

logger = get_logger("main")

ROOT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
FINAL_DIR = ROOT_DIR / "data" / "final"
MODELS_DIR = ROOT_DIR / "models"
RESULTS_DIR = ROOT_DIR / "results"
EDA_DIR = RESULTS_DIR / "eda"

AVAILABLE_TARGETS = [
    "price_regular_milled",
    "price_well_milled",
    "price_superior_milled",
    "price_premium",
    "price_special",
]
DEFAULT_TARGET = "price_regular_milled"


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Rice Price Prediction Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="tui",
        choices=[
            "tui", "status", "merge", "preprocess", "eda",
            "train", "evaluate", "report", "full", "eval-only",
        ],
        help="Action to run. Default: tui (interactive menu).",
    )
    parser.add_argument(
        "--target",
        type=str,
        default=DEFAULT_TARGET,
        help=(
            "Target rice price column to predict.\n"
            f"Options: {AVAILABLE_TARGETS}\n"
            f"Default: {DEFAULT_TARGET}"
        ),
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Disable RandomizedSearchCV (faster, uses default hyperparameters).",
    )
    parser.add_argument(
        "--no-shap",
        action="store_true",
        help="Skip SHAP feature contribution analysis.",
    )
    parser.add_argument(
        "--lstm-epochs",
        type=int,
        default=100,
        help="Maximum LSTM training epochs (default: 100).",
    )
    parser.add_argument(
        "--n-iter",
        type=int,
        default=20,
        help="RandomizedSearchCV iterations for tree models (default: 20).",
    )
    parser.add_argument(
        "--force-merge",
        action="store_true",
        help="Re-run data merge even if merged_dataset.csv already exists.",
    )
    parser.add_argument(
        "--force-preprocess",
        action="store_true",
        help="Ignore the cached preprocessing bundle and rebuild preprocessing.",
    )
    parser.add_argument(
        "--reuse-cache",
        action="store_true",
        help="Prefer cached merged/preprocessed artefacts when available.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Global random seed (default: 42).",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _path_status(path: Path) -> str:
    return "yes" if path.exists() else "no"


def _prompt(text: str, default: Optional[str] = None) -> str:
    try:
        raw = input(text).strip()
    except EOFError:
        return default or ""
    return raw if raw else (default or "")


def _prompt_yes_no(text: str, default: bool = True) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        answer = _prompt(f"{text} {suffix} ", "y" if default else "n").lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer y or n.")


def _show_banner():
    logger.info("╔" + "═" * 53 + "╗")
    logger.info("║  🌾  RICE PRICE PREDICTION PIPELINE                  ║")
    logger.info("╚" + "═" * 53 + "╝")


def _price_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith("price_")]


def _show_target_completeness(df: pd.DataFrame) -> dict[str, float]:
    price_cols = _price_columns(df)
    stats = {}
    if not price_cols:
        print("\nNo rice price columns found in the merged dataset.\n")
        return stats

    print("\nRice commodity completeness")
    print("-" * 60)
    rows = []
    for col in price_cols:
        pct = float(df[col].notna().mean() * 100)
        stats[col] = pct
        rows.append((col, pct))

    rows.sort(key=lambda x: x[1], reverse=True)
    for idx, (col, pct) in enumerate(rows, start=1):
        marker = "(recommended)" if idx == 1 else ""
        print(f"{idx:>2}. {col:<24} {pct:6.1f}% {marker}")
    print("-" * 60)
    return stats


def choose_target(df: pd.DataFrame, current_target: str = DEFAULT_TARGET) -> str:
    price_cols = _price_columns(df)
    if not price_cols:
        logger.warning("No price columns found; keeping current target.")
        return current_target

    completeness = {col: float(df[col].notna().mean() * 100) for col in price_cols}
    ordered = sorted(price_cols, key=lambda c: completeness[c], reverse=True)
    default_choice = current_target if current_target in ordered else ordered[0]

    print("\nSelect rice commodity target")
    print("-" * 60)
    for i, col in enumerate(ordered, start=1):
        rec = " (recommended)" if i == 1 else ""
        cur = " [current]" if col == current_target else ""
        print(f"{i}. {col:<24} {completeness[col]:6.1f}%{rec}{cur}")
    print("-" * 60)
    print(f"Press Enter to keep current selection: {default_choice}")

    while True:
        choice = _prompt("Choose option: ", "").strip()
        if choice == "":
            target = default_choice
            break
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(ordered):
                target = ordered[idx]
                break
        print("Invalid selection. Try again.")

    print(f"Selected target: {target}\n")
    return target


def _artifact_status(target: str) -> dict:
    target_dir = get_target_artifact_dir(target)
    return {
        "merged_dataset": PROCESSED_DIR / "merged_dataset.csv",
        "preprocessed_bundle": get_preprocessed_bundle_path(target),
        "target_dir": target_dir,
        "x_train": target_dir / "X_train.csv",
        "x_test": target_dir / "X_test.csv",
        "scaler": target_dir / "scaler.pkl",
        "lstm_scaler": target_dir / "lstm_scaler.pkl",
        "y_scaler": target_dir / "y_scaler.pkl",
        "models": {
            "rf": MODELS_DIR / "rf.pkl",
            "xgb": MODELS_DIR / "xgb.pkl",
            "hadt": MODELS_DIR / "hadt.pkl",
            "lstm": MODELS_DIR / "lstm.keras",
        },
        "eda_dir": EDA_DIR / target,
        "results_dir": RESULTS_DIR,
        "dashboard": RESULTS_DIR / "report" / "evaluation_dashboard.png",
    }


def print_status(df: pd.DataFrame | None = None, target: str = DEFAULT_TARGET) -> None:
    status = _artifact_status(target)
    print("\nPROJECT STATUS")
    print("-" * 70)
    print(f"Current target       : {target}")
    print(f"Merged dataset       : {status['merged_dataset'].name:<24} {_path_status(status['merged_dataset'])}")
    print(f"Preprocessed bundle  : {status['preprocessed_bundle'].name:<24} {_path_status(status['preprocessed_bundle'])}")
    print(f"Target artefacts dir  : {str(status['target_dir'].relative_to(ROOT_DIR)):<24} {_path_status(status['target_dir'])}")
    print(f"X_train.csv          : {status['x_train'].name:<24} {_path_status(status['x_train'])}")
    print(f"X_test.csv           : {status['x_test'].name:<24} {_path_status(status['x_test'])}")
    print(f"scaler.pkl           : {status['scaler'].name:<24} {_path_status(status['scaler'])}")
    print(f"lstm_scaler.pkl      : {status['lstm_scaler'].name:<24} {_path_status(status['lstm_scaler'])}")
    print(f"y_scaler.pkl         : {status['y_scaler'].name:<24} {_path_status(status['y_scaler'])}")
    print("Models:")
    for name, path in status["models"].items():
        print(f"  - {name.upper():<6} {path.name:<24} {_path_status(path)}")
    print(f"EDA outputs          : {str(status['eda_dir'].relative_to(ROOT_DIR)):<24} {_path_status(status['eda_dir'])}")
    print(f"Results dir          : {str(status['results_dir'].relative_to(ROOT_DIR)):<24} {_path_status(status['results_dir'])}")
    print(f"Dashboard            : {status['dashboard'].name:<24} {_path_status(status['dashboard'])}")
    if df is not None:
        _show_target_completeness(df)
    print("-" * 70)


def step_merge(force: bool = False):
    logger.info("━" * 55)
    logger.info("STEP 1: Merging datasets")
    logger.info("━" * 55)

    merged_path = PROCESSED_DIR / "merged_dataset.csv"
    if merged_path.exists() and not force:
        logger.info(f"Using cached merged dataset: {merged_path}")
        df = pd.read_csv(merged_path)
    else:
        df = merge_datasets()

    logger.info(f"Shape: {df.shape}  |  Years: {df['year'].min()}–{df['year'].max()}")
    return df


def step_preprocess(df, target_col: str, reuse_cache: bool = True, force: bool = False) -> dict:
    logger.info("━" * 55)
    logger.info("STEP 2: Preprocessing")
    logger.info("━" * 55)

    if target_col not in df.columns:
        price_cols = _price_columns(df)
        if not price_cols:
            raise ValueError(
                f"Target '{target_col}' not found. Available columns: {list(df.columns)}"
            )
        logger.warning(
            f"Target '{target_col}' not found – falling back to '{price_cols[0]}'."
        )
        target_col = price_cols[0]

    if reuse_cache and not force:
        cached = load_cached_preprocessed(target_col=target_col)
        if cached is not None:
            logger.info(f"Using cached preprocessing bundle: {get_preprocessed_bundle_path(target_col)}")
            return cached

    logger.info(f"Target column: {target_col}")
    return preprocess(df, target_col=target_col)


def step_eda(df, target_col: str):
    logger.info("━" * 55)
    logger.info("STEP EDA: Exploratory data analysis")
    logger.info("━" * 55)
    return run_eda(df, target_col=target_col, output_dir=EDA_DIR / target_col)


def step_train(preprocessed: dict, args) -> dict:
    logger.info("━" * 55)
    logger.info("STEP 3: Training models")
    logger.info("━" * 55)
    return train_all_models(
        preprocessed,
        tune=not args.no_tune,
        n_iter=args.n_iter,
        lstm_epochs=args.lstm_epochs,
    )


def step_evaluate(models: dict, preprocessed: dict, args) -> tuple:
    logger.info("━" * 55)
    logger.info("STEP 4: Evaluation")
    logger.info("━" * 55)
    predictions = get_all_predictions(models)
    results = full_evaluation(
        predictions=predictions,
        models=models,
        preprocessed=preprocessed,
        feature_names=preprocessed["feature_names"],
        run_shap=not args.no_shap,
    )
    return results, predictions


def step_report(results: dict, predictions: dict, args) -> dict:
    """STEP 5 – Generate dashboard + narrative summary."""
    return run_report(
        predictions=predictions,
        metrics_df=results["metrics"],
        dm_df=results["dm_results"],
        mcs_result=results["mcs_results"],
        target_col=args.target,
    )


def step_eval_only(args):
    """Load pre-trained models from models/ and re-run evaluation."""
    import joblib
    from src.models.train_lstm import predict_lstm

    logger.info("━" * 55)
    logger.info("EVAL-ONLY: Loading saved models")
    logger.info("━" * 55)

    cached = load_cached_preprocessed(target_col=args.target)
    if cached is not None:
        preprocessed = cached
        X_test_np = preprocessed["X_test"]
        y_test = preprocessed["y_test"]
        feature_names = preprocessed["feature_names"]
        X_lstm_test = preprocessed["X_lstm_test"]
        y_lstm_test = preprocessed["y_lstm_test_raw"]
        y_scaler = preprocessed["y_scaler"]
        logger.info(f"Loaded cached preprocessing bundle: {get_preprocessed_bundle_path(args.target)}")
    else:
        target_dir = get_target_artifact_dir(args.target)
        X_train = pd.read_csv(target_dir / "X_train.csv")
        X_test = pd.read_csv(target_dir / "X_test.csv")
        y_test = pd.read_csv(target_dir / "y_test.csv").values.ravel()
        feature_names = list(X_train.columns)
        X_test_np = X_test.values
        X_train_np = X_train.values
        y_scaler = joblib.load(target_dir / "y_scaler.pkl")
        lstm_scaler = joblib.load(target_dir / "lstm_scaler.pkl")
        X_lstm_test = lstm_scaler.transform(X_test_np)
        preprocessed = {
            "X_train": X_train_np,
            "X_train_raw": pd.DataFrame(X_train_np, columns=feature_names),
            "X_lstm_train": lstm_scaler.transform(X_train_np),
            "X_lstm_test": X_lstm_test,
            "feature_names": feature_names,
            "y_scaler": y_scaler,
        }
        y_lstm_test = y_test
        logger.info(f"Loaded fallback CSV splits from {target_dir}")

    models_dict = {}
    predictions = {}

    for name in ("rf", "xgb", "hadt"):
        mp = MODELS_DIR / f"{name}.pkl"
        if mp.exists():
            model = joblib.load(mp)
            models_dict[name] = {
                "model": model,
                "predict_fn": model.predict,
                "X_test": X_test_np,
                "y_test": y_test,
            }
            predictions[name] = {"y_pred": model.predict(X_test_np), "y_test": y_test}
            logger.info(f"Loaded {name.upper()}")
        else:
            logger.warning(f"Not found: {mp}")

    lstm_path = MODELS_DIR / "lstm.keras"
    if lstm_path.exists():
        try:
            import tensorflow as tf
            lstm_model = tf.keras.models.load_model(str(lstm_path))
            preds = predict_lstm(lstm_model, X_lstm_test, y_scaler=y_scaler)
            n = len(preds)
            predictions["lstm"] = {"y_pred": preds, "y_test": y_lstm_test[:n]}
            models_dict["lstm"] = {
                "model": lstm_model,
                "predict_fn": lambda X: predict_lstm(lstm_model, X, y_scaler),
                "X_test": X_lstm_test,
                "y_test": y_lstm_test[:n],
            }
            logger.info("Loaded LSTM")
        except Exception as e:
            logger.warning(f"Could not load LSTM: {e}")

    results = full_evaluation(
        predictions=predictions,
        models=models_dict,
        preprocessed=preprocessed,
        feature_names=feature_names,
        run_shap=not args.no_shap,
    )
    return results, predictions


# ─────────────────────────────────────────────────────────────────────────────
# TUI
# ─────────────────────────────────────────────────────────────────────────────

def _load_or_merge(args, force: bool = False):
    return step_merge(force=force or args.force_merge)


def _load_or_preprocess(df, args, force: bool = False):
    return step_preprocess(
        df,
        args.target,
        reuse_cache=args.reuse_cache,
        force=force or args.force_preprocess,
    )


def tui_loop(args, df: pd.DataFrame):
    while True:
        print_status(df, args.target)
        print("Choose an action:")
        print("  1) Merge datasets")
        print("  2) Preprocess data")
        print("  3) Run EDA")
        print("  4) Train models")
        print("  5) Evaluate trained models")
        print("  6) Generate report")
        print("  7) Run full pipeline")
        print("  8) Eval-only from saved artefacts")
        print("  9) Change rice commodity")
        print("  0) Exit")

        choice = _prompt("Select: ", "9").strip().lower()

        try:
            if choice in {"0", "q", "quit", "exit"}:
                print("Exiting.")
                return
            if choice == "1":
                force = _prompt_yes_no("Re-merge from raw sources?", default=False)
                df = step_merge(force=force)
            elif choice == "2":
                df = _load_or_merge(args, force=False)
                force = _prompt_yes_no("Rebuild preprocessing even if cached bundle exists?", default=False)
                _load_or_preprocess(df, args, force=force)
            elif choice == "3":
                df = _load_or_merge(args, force=False)
                step_eda(df, args.target)
            elif choice == "4":
                df = _load_or_merge(args, force=False)
                force = _prompt_yes_no("Rebuild preprocessing before training?", default=False)
                preprocessed = _load_or_preprocess(df, args, force=force)
                models = step_train(preprocessed, args)
                logger.info(f"Trained models: {list(models.keys())}")
            elif choice == "5":
                step_eval_only(args)
            elif choice == "6":
                results, predictions = step_eval_only(args)
                step_report(results, predictions, args)
            elif choice == "7":
                run_full_pipeline(args)
            elif choice == "8":
                step_eval_only(args)
            elif choice == "9":
                df = _load_or_merge(args, force=False)
                args.target = choose_target(df, args.target)
            else:
                print("Unknown selection. Try again.")
        except Exception as exc:
            logger.exception("Action failed: %s", exc)


def run_full_pipeline(args):
    df = _load_or_merge(args, force=args.force_merge)
    preprocessed = _load_or_preprocess(df, args, force=args.force_preprocess)
    step_eda(df, args.target)
    models = step_train(preprocessed, args)
    results, predictions = step_evaluate(models, preprocessed, args)
    report = step_report(results, predictions, args)

    logger.info("╔" + "═" * 53 + "╗")
    logger.info("║  ✅  PIPELINE COMPLETE                                ║")
    logger.info("╚" + "═" * 53 + "╝")
    logger.info(f"Best model (RMSE) : {results['metrics']['RMSE'].idxmin()}")
    logger.info(f"MCS set           : {results['mcs_results']['mcs_set']}")
    logger.info(f"Dashboard         : {report['dashboard_path']}")
    logger.info("Results saved to  : results/")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    set_seeds(args.seed)

    _show_banner()
    logger.info(f"Target   : {args.target}")
    logger.info(f"Mode     : {args.mode}")
    logger.info(f"Tune     : {not args.no_tune}")
    logger.info(f"SHAP     : {not args.no_shap}")
    logger.info(f"Seed     : {args.seed}")

    if args.mode == "tui":
        df = step_merge(force=args.force_merge)
        args.target = choose_target(df, args.target)
        tui_loop(args, df)
        return
    if args.mode == "status":
        df = step_merge(force=False)
        print_status(df, args.target)
        return
    if args.mode == "merge":
        step_merge(force=args.force_merge)
        return
    if args.mode == "preprocess":
        df = _load_or_merge(args, force=args.force_merge)
        _load_or_preprocess(df, args, force=args.force_preprocess)
        return
    if args.mode == "eda":
        df = _load_or_merge(args, force=args.force_merge)
        step_eda(df, args.target)
        return
    if args.mode == "train":
        df = _load_or_merge(args, force=args.force_merge)
        preprocessed = _load_or_preprocess(df, args, force=args.force_preprocess)
        step_train(preprocessed, args)
        return
    if args.mode == "evaluate":
        df = _load_or_merge(args, force=args.force_merge)
        preprocessed = _load_or_preprocess(df, args, force=args.force_preprocess)
        models = step_train(preprocessed, args)
        step_evaluate(models, preprocessed, args)
        return
    if args.mode == "report":
        df = _load_or_merge(args, force=args.force_merge)
        preprocessed = _load_or_preprocess(df, args, force=args.force_preprocess)
        models = step_train(preprocessed, args)
        results, predictions = step_evaluate(models, preprocessed, args)
        step_report(results, predictions, args)
        return
    if args.mode == "full":
        run_full_pipeline(args)
        return
    if args.mode == "eval-only":
        step_eval_only(args)
        return


if __name__ == "__main__":
    main()
