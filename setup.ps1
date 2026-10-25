# Installation FuelFlow (Windows PowerShell) — lancer depuis le dossier fuelflow
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
New-Item -ItemType Directory -Path data -Force | Out-Null
docker compose up -d
Write-Host "OK. Ouvre 3 terminaux (avec .venv activé) :"
Write-Host "  1) python producer\producer.py"
Write-Host "  2) python consumer\consumer.py"
Write-Host "  3) dbt run --project-dir dbt_project --profiles-dir dbt_project ; streamlit run dashboard\app.py"
