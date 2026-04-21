# Kübler Encoder Cross-Reference Engine (v12 new)

Streamlit app to cross-reference competitor incremental encoders to Kübler matches using a weighted scoring engine.

## Run locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Streamlit Community Cloud

- **Repository:** [`saptakaqb/ai-cross-reference`](https://github.com/saptakaqb/ai-cross-reference)
- **Main file path:** `streamlit_app.py`
- **Python dependencies:** from `requirements.txt`

### Required app data

The app loads records from:

- `data/competitor_unified.csv`

Ensure this file is present in the deployed repository.

### Optional secrets

Use `.streamlit/secrets.toml.template` as reference and set secrets in Streamlit Cloud:

- `ANTHROPIC_API_KEY` (for AI explanation tab)

CockroachDB connectivity is optional and currently used as status-only in UI.
