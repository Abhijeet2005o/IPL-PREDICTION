# 🏏 IPL Match Predictor

AI-powered IPL match prediction using 17 years of data + live ESPNcricinfo JSON API.

## 🔗 Live Demo
[Try the app →](https://ipl-predictor-abhijeet.streamlit.app)

## What It Predicts
| Prediction | Model | Performance |
|-----------|-------|-------------|
| Match winner + probability | XGBoost Classifier | 77.3% accuracy |
| First innings score | XGBoost Regressor | 34.9 RMSE |
| Opener partnership runs | Random Forest | ~37 RMSE |
| Second innings score | XGBoost Regressor | 22.1 RMSE |

## How It Works
1. Click **GET PREDICTION**
2. App fetches today's IPL match from ESPNcricinfo JSON API
3. Gets toss result, Playing XI, venue in real-time
4. ML models predict winner, score, and opener runs

## Tech Stack
Python · XGBoost · Scikit-learn · Streamlit · ESPNcricinfo API · Pandas

## Run Locally
```bash
git clone https://github.com/Abhijeet2005o/ipl-prediction.git
cd ipl-prediction
pip install -r requirements.txt
streamlit run app.py
```

## Author
Abhijeet Panigrahi
