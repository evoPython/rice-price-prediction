import pandas as pd
from pathlib import Path

from src.data.merge_data import merge_datasets
from src.data.preprocess import preprocess
from src.models.pipeline import train_all_models, get_all_predictions
from src.evaluation.evaluate import full_evaluation
from src.evaluation.report import run_report

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "results"

TARGETS = ["price_regular_milled", "price_well_milled", "price_premium"]
CANONICAL = ("price_regular_milled", False)

df = merge_datasets()
print(f"Merged dataset: {df.shape}")

summary_rows = []

for target in TARGETS:
    for remove_outliers in (False, True):
        variant = "outliers_removed" if remove_outliers else "outliers_kept"
        is_canonical = (target, remove_outliers) == CANONICAL
        print(f"\n{'='*70}\nRUN: target={target}  variant={variant}  (save_models={is_canonical})\n{'='*70}")

        preprocessed = preprocess(
            df, target_col=target, remove_outliers=remove_outliers,
            save_scaler=is_canonical, variant=variant,
        )
        models = train_all_models(preprocessed, tune=True, n_iter=20, save=is_canonical)
        predictions = get_all_predictions(models)

        results_dir = RESULTS_DIR / target / variant
        results = full_evaluation(
            predictions=predictions, models=models, preprocessed=preprocessed,
            feature_names=preprocessed["feature_names"], run_shap=True,
            results_dir=results_dir,
        )
        run_report(
            predictions=predictions, metrics_df=results["metrics"],
            dm_df=results["dm_results"], mcs_result=results["mcs_results"],
            target_col=target, report_dir=results_dir / "report",
        )

        for model_name, row in results["metrics"].iterrows():
            summary_rows.append({
                "target": target, "variant": variant, "model": model_name,
                "MAE": row["MAE"], "RMSE": row["RMSE"], "MAPE": row["MAPE"], "ME": row["ME"],
                "n_features": len(preprocessed["feature_names"]),
                "n_train": len(preprocessed["y_train"]), "n_test": len(preprocessed["y_test"]),
                "mcs_set": ",".join(results["mcs_results"]["mcs_set"]),
            })

summary_df = pd.DataFrame(summary_rows)
summary_path = RESULTS_DIR / "experiment_sweep_summary.csv"
summary_df.to_csv(summary_path, index=False)
print(f"\n\nSaved master summary: {summary_path}")
print(summary_df.to_string(index=False))
