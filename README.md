# TalentScore AI — CV Screening Platform

AI-powered CV screening built with FastAPI + OpenAI GPT-4o + MongoDB Atlas.

## Deploy on Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com)

## Environment Variables

Set these in Render dashboard:

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | Your OpenAI API key |
| `MONGO_URI` | MongoDB Atlas connection string |
| `DB_NAME` | Database name (default: talentscore) |

## Local Development

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```
