download the zip files from https://open.eais.go.kr inside `data` subfolder

download `data\법정동코드 전체자료.txt` from https://www.code.go.kr/stdcode/regCodeL.do

---

install python and git (I recommend using https://scoop.sh)

install VS code (or another editor)

run below in powershell

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade --no-cache-dir pip
pip install -r .\requirements.txt
```

restart vs code
