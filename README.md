# 🏏 IPL Match Predictor

AI-powered IPL match prediction using 17 years of data + live ESPNcricinfo JSON API.

## 🔗 Live Demo
[Try the app →](https://ipl-prediction-5v74bcr4xphj5ggvcx789j.streamlit.app/)

## What It Predicts
| Prediction | Model | Performance |
|-----------|-------|-------------|
| Match winner + probability | Voting  | 81.6% accuracy |
| First innings score | XGBoost Regressor | RMSE 24.1 |
| 2nd innings score |  XGBoost Regressor | RMSE 19.6 |
| Powerplay   score |  XGBoost Regressor |  RMSE 16.6 |

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
