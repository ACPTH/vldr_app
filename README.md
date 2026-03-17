# VLDR Generator

Web app to generate Vehicle Loss & Damage Report PDFs from Excel files.

## Local Setup

```bash
pip install -r requirements.txt
# Install pdftk: https://www.pdflabs.com/tools/pdftk-the-pdf-toolkit/
# Place PDF templates in ./templates/
python app.py
# Opens http://127.0.0.1:5050 automatically
```

**Default login:** admin / admin1234 — change immediately in /admin

## Templates needed in ./templates/
- BMW_VLDR.PDF
- Damage_Report_Format.pdf (ECG)
- SCHEDA_VLDR.PDF (FCA)
- VLDR_FORD.PDF
- VLDR_LinkCo.pdf
- PV_Renault.pdf
- Constat_PSA.pdf (Stellantis)
- VGED_VLDR.pdf
- VOLVO_VLDR.pdf

## Deploy to Render

1. Push this repo to GitHub (templates/ included)
2. Go to render.com → New → Web Service
3. Connect your GitHub repo
4. Select "Docker" as runtime
5. Add environment variable: SECRET_KEY (use "Generate" button)
6. Add Disk: mount path /var/data, 1GB
7. Deploy

Notes:
- The app stores users in the file set by `VLDR_DB_PATH` (Render sets to /var/data/users.db in render.yaml).

## Excel Formats Supported
- Internal: sheet=damage_list, columns: vin, make, damage_part_code...
- Report: sheet=damage_list, columns: vehicle_vin, vehicle_make, transport_date...
