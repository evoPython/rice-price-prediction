# 🌾 Rice Price Prediction – ML Pipeline

Forecasting retail rice prices in Cebu / Region VII using four models:
**Random Forest**, **XGBoost**, **HADT**, and **LSTM**.

---

## Project Structure

```
rice-price-prediction/
│
├── data/
│   ├── raw/                        ← place the raw CSVs here (see below)
│   │   ├── Price Data/
│   │   │   └── wfp_food_prices_phl.csv
│   │   ├── Rice Factors Data/
│   │   │   ├── PALAY_PRODUCTION_quarterly.csv
│   │   │   ├── FERTILIZER_PRICES_monthly.csv
│   │   │   ├── YIELD_bi-annual.csv
│   │   │   └── RICE_AREA_bi-annual.csv
│   │   └── Weather Data/
│   │       └── Mactan Monthly Data.csv
│   ├── processed/                  ← auto-generated: merged_dataset.csv
│   └── final/                      ← auto-generated: train/test splits + scalers
│
├── notebooks/
│   ├── 01_eda.ipynb
│   └── 02_preprocessing.ipynb
│
├── src/
│   ├── data/
│   │   ├── load_data.py            – loaders for all 6 raw files
│   │   ├── merge_data.py           – joins all sources on (year, month)
│   │   └── preprocess.py          – interpolation, IQR, scaling, 80/20 split
│   ├── models/
│   │   ├── train_rf.py             – Random Forest + RandomizedSearchCV
│   │   ├── train_xgb.py            – XGBoost + early stopping
│   │   ├── train_hadt.py           – Hybrid Association Decision Tree
│   │   ├── train_lstm.py           – Stacked LSTM + adaptive callbacks
│   │   └── pipeline.py             – unified trainer for all four models
│   ├── evaluation/
│   │   ├── metrics.py              – MAE, RMSE, MAPE, ME
│   │   ├── statistical_tests.py    – Diebold-Mariano + Model Confidence Set
│   │   └── evaluate.py             – full evaluation + SHAP + result plots
│   └── utils/
│       └── helpers.py              – logger, model persistence, seed fixing
│
├── models/                         ← saved .pkl / .keras model files
├── results/                        ← metrics, plots, SHAP outputs
├── main.py                         ← 🚀 entry point
└── requirements.txt
```

---

## Raw Data Sources

| File | Source | Coverage |
|------|--------|----------|
| `wfp_food_prices_phl.csv` | WFP HDX | Retail rice prices, Cebu City, 2000–2025 |
| `PALAY_PRODUCTION_quarterly.csv` | PSA | Irrigated + Rainfed palay production, Cebu, 1987–2025 |
| `FERTILIZER_PRICES_monthly.csv` | FPA / DA | Region VII fertilizer prices, 2021–2025 |
| `YIELD_bi-annual.csv` | PRISM PhilRice | Average yield (t/ha), Region VII, 2018–2025 |
| `RICE_AREA_bi-annual.csv` | PRISM PhilRice | Harvested area (ha), Region VII, 2018–2025 |
| `Mactan Monthly Data.csv` | PAGASA / Mactan station | Temperature, rainfall, humidity, wind, 2015–2024 |

---

## Setup

```bash
# 1. Create a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt
```

---

## Running the Pipeline

```bash
# Full pipeline — merge data, preprocess, train all models, evaluate
python main.py

# Faster development run (no tuning, no SHAP)
python main.py --no-tune --no-shap

# Choose a different target rice classification
python main.py --target price_regular_milled

# Force re-merge (if you've updated raw data)
python main.py --force-merge

# Re-run evaluation only (uses saved models in models/)
python main.py --eval-only
```

### Available target columns

| Flag value | Description |
|------------|-------------|
| `price_well_milled` *(default)* | Well-milled rice retail price |
| `price_regular_milled` | Regular-milled rice retail price |
| `price_superior_milled` | Superior-milled rice retail price |
| `price_premium` | Premium rice retail price |
| `price_special` | Special rice retail price |

### All CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | `price_well_milled` | Target price column |
| `--no-tune` | off | Skip RandomizedSearchCV |
| `--no-shap` | off | Skip SHAP analysis |
| `--lstm-epochs` | 100 | Max LSTM training epochs |
| `--n-iter` | 20 | Hyperparameter search iterations |
| `--eval-only` | off | Skip training; load saved models |
| `--force-merge` | off | Re-merge even if CSV already exists |
| `--seed` | 42 | Global random seed |

---

## Outputs

| Path | Contents |
|------|----------|
| `data/processed/merged_dataset.csv` | All sources joined on (year, month) |
| `data/final/X_train.csv` / `X_test.csv` | Scaled feature splits |
| `data/final/y_train.csv` / `y_test.csv` | Target splits |
| `data/final/scaler.pkl` etc. | Fitted StandardScaler objects |
| `models/rf.pkl` | Fitted Random Forest |
| `models/xgb.pkl` | Fitted XGBoost |
| `models/hadt.pkl` | Fitted HADT |
| `models/lstm.keras` | Fitted LSTM |
| `results/metrics.csv` | MAE / RMSE / MAPE / ME per model |
| `results/dm_tests.csv` | Pairwise Diebold-Mariano results |
| `results/evaluation_results.json` | All metrics + MCS output |
| `results/plots/` | Actual vs predicted, time-series, metrics bar charts |
| `results/shap/` | SHAP bar plots + `shap_importances.csv` |

---

## Model Details

### HADT (Hybrid Association Decision Tree)
Two-stage hybrid (`src/models/train_hadt.py`):
1. **Stage 1** – Mutual information scores weight each feature (association-rule analogue).
2. **Stage 2** – AdaBoostRegressor over Decision Tree base learners trained on the weighted matrix.

### LSTM
Stacked LSTM (`src/models/train_lstm.py`):
- Two stacked LSTM layers (64 → 32 units), Dropout after each
- ReduceLROnPlateau + EarlyStopping + ModelCheckpoint callbacks
- Trained on the outlier-cleaned data subset; target is inverse-scaled for evaluation

---

## Statistical Model Selection
1. **Diebold-Mariano test** – pairwise significance test with Newey-West HAC variance and Harvey-Leybourne-Newbold small-sample correction.
2. **Model Confidence Set (MCS)** – bootstrap range statistic (α = 0.10) iteratively eliminates the worst model until all survivors are statistically equivalent.

---

## Reproducibility
All seeds are fixed via `--seed` (numpy, Python random, TensorFlow). Default seed = 42.
